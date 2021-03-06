/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.exec.vector.VectorExtractRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomBatchSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIf;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFWhen;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import junit.framework.Assert;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Ignore;
import org.junit.Test;

public class TestVectorIfStatement {

  @Test
  public void testBoolean() throws Exception {
    Random random = new Random(12882);

    doIfTests(random, "boolean");
  }

  @Test
  public void testInt() throws Exception {
    Random random = new Random(12882);

    doIfTests(random, "int");
  }

  @Test
  public void testBigInt() throws Exception {
    Random random = new Random(12882);

    doIfTests(random, "bigint");
  }

  @Test
  public void testString() throws Exception {
    Random random = new Random(12882);

    doIfTests(random, "string");
  }

  @Test
  public void testTimestamp() throws Exception {
    Random random = new Random(12882);

    doIfTests(random, "timestamp");
  }

  @Test
  public void testDate() throws Exception {
    Random random = new Random(12882);

    doIfTests(random, "date");
  }

  @Test
  public void testIntervalDayTime() throws Exception {
    Random random = new Random(12882);

    //VectorUDFAdapter doesn't support interval_day_time
    doIfTests(random, "interval_day_time",
        Arrays.asList(IfStmtTestMode.ROW_MODE, IfStmtTestMode.VECTOR_EXPRESSION));
  }

  @Test
  public void testIntervalYearMonth() throws Exception {
    Random random = new Random(12882);

    //VectorUDFAdapter doesn't support interval_year_month
    doIfTests(random, "interval_year_month",
        Arrays.asList(IfStmtTestMode.ROW_MODE, IfStmtTestMode.VECTOR_EXPRESSION));
  }

  @Test
  public void testDouble() throws Exception {
    Random random = new Random(12882);

    doIfTests(random, "double");
  }

  @Test
  public void testChar() throws Exception {
    Random random = new Random(12882);

    doIfTests(random, "char(10)");
  }

  @Test
  public void testVarchar() throws Exception {
    Random random = new Random(12882);

    doIfTests(random, "varchar(15)");
  }

  @Ignore("There is no vector expression for binary if expressions and vector adapter doesn't support it")
  @Test
  public void testBinary() throws Exception {
    Random random = new Random(12882);

    //VectorUDFAdapter doesn't support binary
    doIfTests(random, "binary",
        Arrays.asList(IfStmtTestMode.ROW_MODE, IfStmtTestMode.VECTOR_EXPRESSION));
  }

  @Test
  public void testDecimalLarge() throws Exception {
    Random random = new Random(9300);

    doIfTests(random, "decimal(20,8)");
  }

  @Test
  public void testDecimalSmall() throws Exception {
    Random random = new Random(12882);

    doIfTests(random, "decimal(10,4)");
  }

  public enum IfStmtTestMode {
    ROW_MODE,
    ADAPTOR_WHEN,
    VECTOR_EXPRESSION;

    static final int count = values().length;
  }

  public enum ColumnScalarMode {
    COLUMN_COLUMN,
    COLUMN_SCALAR,
    SCALAR_COLUMN,
    SCALAR_SCALAR;

    static final int count = values().length;
  }

  private void doIfTests(Random random, String typeName)
      throws Exception {
    doIfTests(random, typeName, null);
  }

  private void doIfTests(Random random, String typeName, List<IfStmtTestMode> testIfStmtTestModes)
          throws Exception {
    for (ColumnScalarMode columnScalarMode : ColumnScalarMode.values()) {
      doIfTestsWithDiffColumnScalar(
          random, typeName, columnScalarMode, testIfStmtTestModes);
    }
  }

  private void doIfTestsWithDiffColumnScalar(Random random, String typeName,
      ColumnScalarMode columnScalarMode, List<IfStmtTestMode> testIfStmtTestModes)
      throws Exception {
    doIfTestsWithDiffColumnScalar(
        random, typeName, ColumnScalarMode.COLUMN_COLUMN, testIfStmtTestModes, false, false);
    doIfTestsWithDiffColumnScalar(
        random, typeName, ColumnScalarMode.COLUMN_SCALAR, testIfStmtTestModes, false, false);
    doIfTestsWithDiffColumnScalar(
        random, typeName, ColumnScalarMode.COLUMN_SCALAR, testIfStmtTestModes, false, true);
    doIfTestsWithDiffColumnScalar(
        random, typeName, ColumnScalarMode.SCALAR_COLUMN, testIfStmtTestModes, false, false);
    doIfTestsWithDiffColumnScalar(
        random, typeName, ColumnScalarMode.SCALAR_COLUMN, testIfStmtTestModes, true, false);
    doIfTestsWithDiffColumnScalar(
        random, typeName, ColumnScalarMode.SCALAR_SCALAR, testIfStmtTestModes, false, false);
  }

  private void doIfTestsWithDiffColumnScalar(Random random, String typeName,
      ColumnScalarMode columnScalarMode, List<IfStmtTestMode> testIfStmtTestModes,
      boolean isNullScalar1, boolean isNullScalar2)
          throws Exception {

    System.out.println("*DEBUG* typeName " + typeName +
        " columnScalarMode " + columnScalarMode +
        " isNullScalar1 " + isNullScalar1 +
        " isNullScalar2 " + isNullScalar2);

    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeName);

    List<String> explicitTypeNameList = new ArrayList<String>();
    explicitTypeNameList.add("boolean");
    if (columnScalarMode != ColumnScalarMode.SCALAR_SCALAR) {
      explicitTypeNameList.add(typeName);
      if (columnScalarMode == ColumnScalarMode.COLUMN_COLUMN) {
        explicitTypeNameList.add(typeName);
      }
    }

    VectorRandomRowSource rowSource = new VectorRandomRowSource();

    rowSource.initExplicitSchema(
        random, explicitTypeNameList, /* maxComplexDepth */ 0, /* allowNull */ true);

    List<String> columns = new ArrayList<String>();
    columns.add("col0");    // The boolean predicate.

    ExprNodeColumnDesc col1Expr = new  ExprNodeColumnDesc(Boolean.class, "col0", "table", false);
    int columnNum = 1;
    ExprNodeDesc col2Expr;
    if (columnScalarMode == ColumnScalarMode.COLUMN_COLUMN ||
        columnScalarMode == ColumnScalarMode.COLUMN_SCALAR) {
      String columnName = "col" + (columnNum++);
      col2Expr = new ExprNodeColumnDesc(typeInfo, columnName, "table", false);
      columns.add(columnName);
    } else {
      Object scalar1Object;
      if (isNullScalar1) {
        scalar1Object = null;
      } else {
        scalar1Object =
            VectorRandomRowSource.randomPrimitiveObject(
                random, (PrimitiveTypeInfo) typeInfo);
      }
      col2Expr = new ExprNodeConstantDesc(typeInfo, scalar1Object);
    }
    ExprNodeDesc col3Expr;
    if (columnScalarMode == ColumnScalarMode.COLUMN_COLUMN ||
        columnScalarMode == ColumnScalarMode.SCALAR_COLUMN) {
      String columnName = "col" + (columnNum++);
      col3Expr = new ExprNodeColumnDesc(typeInfo, columnName, "table", false);
      columns.add(columnName);
    } else {
      Object scalar2Object;
      if (isNullScalar2) {
        scalar2Object = null;
      } else {
        scalar2Object =
            VectorRandomRowSource.randomPrimitiveObject(
                random, (PrimitiveTypeInfo) typeInfo);
      }
      col3Expr = new ExprNodeConstantDesc(typeInfo, scalar2Object);
    }

    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
    children.add(col1Expr);
    children.add(col2Expr);
    children.add(col3Expr);

    //----------------------------------------------------------------------------------------------

    String[] columnNames = columns.toArray(new String[0]);

    String[] outputScratchTypeNames = new String[] { typeName };

    VectorizedRowBatchCtx batchContext =
        new VectorizedRowBatchCtx(
            columnNames,
            rowSource.typeInfos(),
            /* dataColumnNums */ null,
            /* partitionColumnCount */ 0,
            outputScratchTypeNames);

    Object[][] randomRows = rowSource.randomRows(100000);

    VectorRandomBatchSource batchSource =
        VectorRandomBatchSource.createInterestingBatches(
            random,
            rowSource,
            randomRows,
            null);

    final int rowCount = randomRows.length;
    List<IfStmtTestMode> testModes;
    if (testIfStmtTestModes == null) {
      testModes = Arrays.asList(IfStmtTestMode.values());
    } else {
      testModes = testIfStmtTestModes;
    }
    Object[][] resultObjectsArray = new Object[testModes.size()][];
    for (int i = 0; i < testModes.size(); i++) {

      Object[] resultObjects = new Object[rowCount];
      resultObjectsArray[i] = resultObjects;

      IfStmtTestMode ifStmtTestMode = testModes.get(i);
      switch (ifStmtTestMode) {
      case ROW_MODE:
        doRowIfTest(
            typeInfo, columns, children, randomRows, rowSource.rowStructObjectInspector(),
            resultObjects);
        break;
      case ADAPTOR_WHEN:
      case VECTOR_EXPRESSION:
        doVectorIfTest(
            typeInfo,
            columns,
            rowSource.typeInfos(),
            children,
            ifStmtTestMode,
            columnScalarMode,
            batchSource,
            batchContext,
            resultObjects);
        break;
      default:
        throw new RuntimeException("Unexpected IF statement test mode " + ifStmtTestMode);
      }
    }

    for (int i = 0; i < rowCount; i++) {
      // Row-mode is the expected value.
      Object expectedResult = resultObjectsArray[0][i];

      for (int v = 1; v < testModes.size(); v++) {
        Object vectorResult = resultObjectsArray[v][i];
        if (expectedResult == null || vectorResult == null) {
          if (expectedResult != null || vectorResult != null) {
            Assert.fail(
                "Row " + i + " " + testModes.get(v) +
                " " + columnScalarMode +
                " result is NULL " + (vectorResult == null) +
                " does not match row-mode expected result is NULL " + (expectedResult == null));
          }
        } else {

          if (!expectedResult.equals(vectorResult)) {
            Assert.fail(
                "Row " + i + " " + IfStmtTestMode.values()[v] +
                " " + columnScalarMode +
                " result " + vectorResult.toString() +
                " (" + vectorResult.getClass().getSimpleName() + ")" +
                " does not match row-mode expected result " + expectedResult.toString() +
                " (" + expectedResult.getClass().getSimpleName() + ")");
          }
        }
      }
    }
  }

  private void doRowIfTest(TypeInfo typeInfo, List<String> columns, List<ExprNodeDesc> children,
      Object[][] randomRows, ObjectInspector rowInspector, Object[] resultObjects) throws Exception {

    GenericUDF udf = new GenericUDFIf();

    ExprNodeGenericFuncDesc exprDesc =
        new ExprNodeGenericFuncDesc(typeInfo, udf, children);
    ExprNodeEvaluator evaluator =
        ExprNodeEvaluatorFactory.get(exprDesc);
    evaluator.initialize(rowInspector);

    final int rowCount = randomRows.length;
    for (int i = 0; i < rowCount; i++) {
      Object[] row = randomRows[i];
      Object result = evaluator.evaluate(row);
      resultObjects[i] = result;
    }
  }

  private void extractResultObjects(VectorizedRowBatch batch, int rowIndex,
      VectorExtractRow resultVectorExtractRow, Object[] scrqtchRow, Object[] resultObjects,
      int outputColIndex, TypeInfo outputTypeInfo) {
    ObjectInspector objectInspector = TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(outputTypeInfo);

    boolean selectedInUse = batch.selectedInUse;
    int[] selected = batch.selected;
    for (int logicalIndex = 0; logicalIndex < batch.size; logicalIndex++) {
      final int batchIndex = (selectedInUse ? selected[logicalIndex] : logicalIndex);
      Object extractedRow =
          resultVectorExtractRow.extractRowColumn(batch, batchIndex, outputColIndex);

      Object copyResult = ObjectInspectorUtils
          .copyToStandardObject(extractedRow, objectInspector, ObjectInspectorCopyOption.WRITABLE);
      resultObjects[rowIndex++] = copyResult;
    }
  }

/*
  private Object getCopyOf(Object o, TypeInfo info) {
    if (info instanceof PrimitiveTypeInfo) {
      if (o == null) {
         return o;
      }
      PrimitiveCategory category = ((PrimitiveTypeInfo) info).getPrimitiveCategory();
      Writable result = VectorizedBatchUtil.getPrimitiveWritable(category);
      switch (category) {
      case VOID:
        return null;
      case BOOLEAN:
        return new BooleanWritable(((BooleanWritable) o).get());
      case BYTE:
        return new ByteWritable(((ByteWritable) o).get());
      case SHORT:
        return new ShortWritable(((ShortWritable) o).get());
      case INT:
        return new IntWritable(((IntWritable) o).get());
      case LONG:
        return new LongWritable(((LongWritable) o).get());
      case TIMESTAMP:
        return new TimestampWritable(((TimestampWritable) o));
      case DATE:
        return new DateWritable(((DateWritable) o).get());
      case FLOAT:
        return new FloatWritable(((FloatWritable) o).get());
      case DOUBLE:
        return new DoubleWritable(((DoubleWritable) o).get());
      case BINARY:
        return new BytesWritable(((BytesWritable) o).get());
      case STRING:
        Text ret = new Text();
        ret.set(((Text) o).getBytes(), 0, ((Text) o).getLength());
        return ret;
      case VARCHAR:
        return new HiveVarcharWritable(((HiveVarcharWritable) o));
      case CHAR:
        return new HiveCharWritable(((HiveCharWritable) o));
      case DECIMAL:
        return new HiveDecimalWritable(((HiveDecimalWritable) o));
      case INTERVAL_YEAR_MONTH:
        return new HiveIntervalYearMonthWritable(((HiveIntervalYearMonthWritable) o));
      case INTERVAL_DAY_TIME:
        return new HiveIntervalDayTimeWritable(((HiveIntervalDayTimeWritable) o));
      default:
        throw new RuntimeException("Unexpected type found" + info.getTypeName());
      }
    }
    throw new RuntimeException("Unexpected type found" + info.getTypeName());
  }
*/
  private void doVectorIfTest(TypeInfo typeInfo, List<String> columns, TypeInfo[] typeInfos,
      List<ExprNodeDesc> children, IfStmtTestMode ifStmtTestMode, ColumnScalarMode columnScalarMode,
      VectorRandomBatchSource batchSource, VectorizedRowBatchCtx batchContext,
      Object[] resultObjects) throws Exception {

    GenericUDF udf;
    switch (ifStmtTestMode) {
    case VECTOR_EXPRESSION:
      udf = new GenericUDFIf();
      break;
    case ADAPTOR_WHEN:
      udf = new GenericUDFWhen();
      break;
    default:
      throw new RuntimeException("Unexpected IF statement test mode " + ifStmtTestMode);
    }

    ExprNodeGenericFuncDesc exprDesc = new ExprNodeGenericFuncDesc(typeInfo, udf, children);

    HiveConf hiveConf = new HiveConf();
    VectorizationContext vectorizationContext = new VectorizationContext("name", columns, hiveConf);
    VectorExpression vectorExpression = vectorizationContext.getVectorExpression(exprDesc);

    VectorizedRowBatch batch = batchContext.createVectorizedRowBatch();

    VectorExtractRow resultVectorExtractRow = new VectorExtractRow();
    List<String> rowTypeNames = new ArrayList<>(batchSource.getRowSource().typeNames());
    rowTypeNames.add(typeInfo.getTypeName());
    resultVectorExtractRow.init(rowTypeNames);
    Object[] scrqtchRow = new Object[rowTypeNames.size()];

    System.out.println(
        "*DEBUG* typeInfo " + typeInfo.toString() +
        " ifStmtTestMode " + ifStmtTestMode +
        " columnScalarMode " + columnScalarMode +
        " vectorExpression " + vectorExpression.getClass().getSimpleName());

    batchSource.resetBatchIteration();
    int rowIndex = 0;
    while (true) {
      if (!batchSource.fillNextBatch(batch)) {
        break;
      }
      vectorExpression.evaluate(batch);
      extractResultObjects(batch, rowIndex, resultVectorExtractRow, scrqtchRow, resultObjects,
          vectorExpression.getOutputColumn(), typeInfo);
      rowIndex += batch.size;
    }
  }
}
