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


import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.exec.vector.VectorExtractRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomBatchSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource.GenerationSpec;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPDivide;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPMinus;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPMod;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPMultiply;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPPlus;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;;

import junit.framework.Assert;

import org.junit.Test;

public class TestVectorArithmetic {

  public TestVectorArithmetic() {
    // Arithmetic operations rely on getting conf from SessionState, need to initialize here.
    SessionState ss = new SessionState(new HiveConf());
    ss.getConf().setVar(HiveConf.ConfVars.HIVE_COMPAT, "latest");
    SessionState.setCurrentSessionState(ss);
  }

  @Test
  public void testIntegers() throws Exception {
    Random random = new Random(7743);

    doIntegerTests(random);
  }

  @Test
  public void testIntegerFloating() throws Exception {
    Random random = new Random(7743);

    doIntegerFloatingTests(random);
  }

  @Test
  public void testFloating() throws Exception {
    Random random = new Random(7743);

    doFloatingTests(random);
  }

  @Test
  public void testDecimals() throws Exception {
    Random random = new Random(7743);

    doDecimalTests(random);
  }

  public enum ArithmeticTestMode {
    ROW_MODE,
    ADAPTOR,
    VECTOR_EXPRESSION;

    static final int count = values().length;
  }

  public enum ColumnScalarMode {
    COLUMN_COLUMN,
    COLUMN_SCALAR,
    SCALAR_COLUMN;

    static final int count = values().length;
  }

  private static TypeInfo[] integerTypeInfos = new TypeInfo[] {
    TypeInfoFactory.byteTypeInfo,
    TypeInfoFactory.shortTypeInfo,
    TypeInfoFactory.intTypeInfo,
    TypeInfoFactory.longTypeInfo
  };

  // We have test failures with FLOAT.  Ignoring this issue for now.
  private static TypeInfo[] floatingTypeInfos = new TypeInfo[] {
    // TypeInfoFactory.floatTypeInfo,
    TypeInfoFactory.doubleTypeInfo
  };

  private void doIntegerTests(Random random)
          throws Exception {
    for (TypeInfo typeInfo : integerTypeInfos) {
      for (ColumnScalarMode columnScalarMode : ColumnScalarMode.values()) {
        doTestsWithDiffColumnScalar(
            random, typeInfo, typeInfo, columnScalarMode);
      }
    }
  }

  private void doIntegerFloatingTests(Random random)
      throws Exception {
    for (TypeInfo typeInfo1 : integerTypeInfos) {
      for (TypeInfo typeInfo2 : floatingTypeInfos) {
        for (ColumnScalarMode columnScalarMode : ColumnScalarMode.values()) {
          doTestsWithDiffColumnScalar(
              random, typeInfo1, typeInfo2, columnScalarMode);
        }
      }
    }
    for (TypeInfo typeInfo1 : floatingTypeInfos) {
      for (TypeInfo typeInfo2 : integerTypeInfos) {
        for (ColumnScalarMode columnScalarMode : ColumnScalarMode.values()) {
          doTestsWithDiffColumnScalar(
              random, typeInfo1, typeInfo2, columnScalarMode);
        }
      }
    }
  }

  private void doFloatingTests(Random random)
      throws Exception {
    for (TypeInfo typeInfo1 : floatingTypeInfos) {
      for (TypeInfo typeInfo2 : floatingTypeInfos) {
        for (ColumnScalarMode columnScalarMode : ColumnScalarMode.values()) {
          doTestsWithDiffColumnScalar(
              random, typeInfo1, typeInfo2, columnScalarMode);
        }
      }
    }
  }

  private static TypeInfo[] decimalTypeInfos = new TypeInfo[] {
    new DecimalTypeInfo(38, 18),
    new DecimalTypeInfo(25, 2),
    new DecimalTypeInfo(19, 4),
    new DecimalTypeInfo(18, 10),
    new DecimalTypeInfo(17, 3),
    new DecimalTypeInfo(12, 2),
    new DecimalTypeInfo(7, 1)
  };

  private void doDecimalTests(Random random)
      throws Exception {
    for (TypeInfo typeInfo : decimalTypeInfos) {
      for (ColumnScalarMode columnScalarMode : ColumnScalarMode.values()) {
        doTestsWithDiffColumnScalar(
            random, typeInfo, typeInfo, columnScalarMode);
      }
    }
  }

  public enum Arithmetic {
    ADD,
    SUBTRACT,
    MULTIPLY,
    DIVIDE,
    MODULUS;
  }

  private TypeInfo getDecimalScalarTypeInfo(Object scalarObject) {
    HiveDecimal dec = (HiveDecimal) scalarObject;
    int precision = dec.precision();
    int scale = dec.scale();
    return new DecimalTypeInfo(precision, scale);
  }

  private void doTestsWithDiffColumnScalar(Random random, TypeInfo typeInfo1, TypeInfo typeInfo2,
      ColumnScalarMode columnScalarMode)
          throws Exception {
    for (Arithmetic arithmetic : Arithmetic.values()) {
      doTestsWithDiffColumnScalar(random, typeInfo1, typeInfo2, columnScalarMode, arithmetic);
    }
  }

  private void doTestsWithDiffColumnScalar(Random random, TypeInfo typeInfo1, TypeInfo typeInfo2,
      ColumnScalarMode columnScalarMode, Arithmetic arithmetic)
          throws Exception {

    String typeName1 = typeInfo1.getTypeName();
    List<GenerationSpec> generationSpecList = new ArrayList<GenerationSpec>();

    List<String> columns = new ArrayList<String>();
    int columnNum = 0;

    ExprNodeDesc col1Expr;
    Object scalar1Object = null;
    if (columnScalarMode == ColumnScalarMode.COLUMN_COLUMN ||
        columnScalarMode == ColumnScalarMode.COLUMN_SCALAR) {
      generationSpecList.add(
          GenerationSpec.createSameType(typeInfo1));

      String columnName = "col" + (columnNum++);
      col1Expr = new ExprNodeColumnDesc(typeInfo1, columnName, "table", false);
      columns.add(columnName);
    } else {
      scalar1Object =
          VectorRandomRowSource.randomPrimitiveObject(
              random, (PrimitiveTypeInfo) typeInfo1);

      // Adjust the decimal type to the scalar's type...
      if (typeInfo1 instanceof DecimalTypeInfo) {
        typeInfo1 = getDecimalScalarTypeInfo(scalar1Object);
      }

      col1Expr = new ExprNodeConstantDesc(typeInfo1, scalar1Object);
    }
    ExprNodeDesc col2Expr;
    Object scalar2Object = null;
    if (columnScalarMode == ColumnScalarMode.COLUMN_COLUMN ||
        columnScalarMode == ColumnScalarMode.SCALAR_COLUMN) {
      generationSpecList.add(
          GenerationSpec.createSameType(typeInfo2));

      String columnName = "col" + (columnNum++);
      col2Expr = new ExprNodeColumnDesc(typeInfo2, columnName, "table", false);
      columns.add(columnName);
    } else {
      scalar2Object =
          VectorRandomRowSource.randomPrimitiveObject(
              random, (PrimitiveTypeInfo) typeInfo2);

      // Adjust the decimal type to the scalar's type...
      if (typeInfo2 instanceof DecimalTypeInfo) {
        typeInfo2 = getDecimalScalarTypeInfo(scalar2Object);
      }

      col2Expr = new ExprNodeConstantDesc(typeInfo2, scalar2Object);
    }

    List<ObjectInspector> objectInspectorList = new ArrayList<ObjectInspector>();
    objectInspectorList.add(VectorRandomRowSource.getObjectInspector(typeInfo1));
    objectInspectorList.add(VectorRandomRowSource.getObjectInspector(typeInfo2));

    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
    children.add(col1Expr);
    children.add(col2Expr);

    //----------------------------------------------------------------------------------------------

    String[] columnNames = columns.toArray(new String[0]);

    VectorRandomRowSource rowSource = new VectorRandomRowSource();

    rowSource.initGenerationSpecSchema(
        random, generationSpecList, /* maxComplexDepth */ 0, /* allowNull */ true);

    Object[][] randomRows = rowSource.randomRows(100000);

    VectorRandomBatchSource batchSource =
        VectorRandomBatchSource.createInterestingBatches(
            random,
            rowSource,
            randomRows,
            null);

    GenericUDF genericUdf;

    switch (arithmetic) {
      case ADD:
        genericUdf = new GenericUDFOPPlus();
        break;
      case SUBTRACT:
        genericUdf = new GenericUDFOPMinus();
        break;
      case MULTIPLY:
        genericUdf = new GenericUDFOPMultiply();
        break;
      case DIVIDE:
        genericUdf = new GenericUDFOPDivide();
        break;
      case MODULUS:
        genericUdf = new GenericUDFOPMod();
        break;
      default:
        throw new RuntimeException("Unexpected arithmetic " + arithmetic);
    }

    ObjectInspector[] objectInspectors =
        objectInspectorList.toArray(new ObjectInspector[objectInspectorList.size()]);
    ObjectInspector outputObjectInspector = null;

    try {
      outputObjectInspector = genericUdf.initialize(objectInspectors);
    } catch (Exception e) {
      Assert.fail(e.toString());
    }

    TypeInfo outputTypeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(outputObjectInspector);

    ExprNodeGenericFuncDesc exprDesc =
        new ExprNodeGenericFuncDesc(outputTypeInfo, genericUdf, children);

    final int rowCount = randomRows.length;
    Object[][] resultObjectsArray = new Object[ArithmeticTestMode.count][];

    for (int i = 0; i < ArithmeticTestMode.count; i++) {
      Object[] resultObjects = new Object[rowCount];
      resultObjectsArray[i] = resultObjects;

      ArithmeticTestMode arithmeticTestMode = ArithmeticTestMode.values()[i];

      switch (arithmeticTestMode) {
        case ROW_MODE:
          doRowArithmeticTest(
              exprDesc,
              randomRows,
              rowSource.rowStructObjectInspector(),
              outputTypeInfo,
              resultObjects);
          break;
        case ADAPTOR:
        case VECTOR_EXPRESSION:
          doVectorArithmeticTest(
              columns,
              columnNames,
              rowSource.typeInfos(),
              exprDesc,
              arithmeticTestMode,
              batchSource,
              exprDesc.getWritableObjectInspector(),
              outputTypeInfo,
              resultObjects);
          break;
        default:
          throw new RuntimeException("Unexpected IF statement test mode " + arithmeticTestMode);
      }
    }

    for (int i = 0; i < rowCount; i++) {
      // Row-mode is the expected value.
      Object expectedResult = resultObjectsArray[0][i];

      for (int v = 1; v < ArithmeticTestMode.count; v++) {
        Object vectorResult = resultObjectsArray[v][i];
        if (expectedResult == null || vectorResult == null) {
          if (expectedResult != null || vectorResult != null) {
            Assert.fail(
                "Row " + i +
                " typeName " + typeName1 +
                " outputTypeName " + outputTypeInfo.getTypeName() +
                " " + arithmetic +
                " " + ArithmeticTestMode.values()[v] +
                " " + columnScalarMode +
                " result is NULL " + (vectorResult == null) +
                " does not match row-mode expected result is NULL " + (expectedResult == null) +
                (columnScalarMode == ColumnScalarMode.SCALAR_COLUMN ?
                    " scalar1 " + scalar1Object.toString() : "") +
                " row values " + Arrays.toString(randomRows[i]) +
                (columnScalarMode == ColumnScalarMode.COLUMN_SCALAR ?
                    " scalar2 " + scalar2Object.toString() : ""));
          }
        } else {

          if (!expectedResult.equals(vectorResult)) {
            Assert.fail(
                "Row " + i +
                " typeName " + typeName1 +
                " outputTypeName " + outputTypeInfo.getTypeName() +
                " " + arithmetic +
                " " + ArithmeticTestMode.values()[v] +
                " " + columnScalarMode +
                " result " + vectorResult.toString() +
                " (" + vectorResult.getClass().getSimpleName() + ")" +
                " does not match row-mode expected result " + expectedResult.toString() +
                " (" + expectedResult.getClass().getSimpleName() + ")" +
                (columnScalarMode == ColumnScalarMode.SCALAR_COLUMN ?
                    " scalar1 " + scalar1Object.toString() : "") +
                " row values " + Arrays.toString(randomRows[i]) +
                (columnScalarMode == ColumnScalarMode.COLUMN_SCALAR ?
                    " scalar2 " + scalar2Object.toString() : ""));
          }
        }
      }
    }
  }

  private void doRowArithmeticTest(
      ExprNodeGenericFuncDesc exprDesc,
      Object[][] randomRows,
      ObjectInspector rowInspector,
      TypeInfo outputTypeInfo, Object[] resultObjects) throws Exception {

    ExprNodeEvaluator evaluator =
        ExprNodeEvaluatorFactory.get(exprDesc);
    evaluator.initialize(rowInspector);

    ObjectInspector objectInspector =
        TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(
            outputTypeInfo);

    final int rowCount = randomRows.length;

    for (int i = 0; i < rowCount; i++) {
      Object[] row = randomRows[i];
      Object result = evaluator.evaluate(row);
      Object copyResult = null;
      try {
        copyResult =
            ObjectInspectorUtils.copyToStandardObject(
                result, objectInspector, ObjectInspectorCopyOption.WRITABLE);
      } catch (Exception e) {
        Assert.fail(e.toString());
      }
      resultObjects[i] = copyResult;
    }
  }

  private void extractResultObjects(VectorizedRowBatch batch, int rowIndex,
      VectorExtractRow resultVectorExtractRow, Object[] scrqtchRow,
      ObjectInspector objectInspector, Object[] resultObjects) {

    boolean selectedInUse = batch.selectedInUse;
    int[] selected = batch.selected;

    for (int logicalIndex = 0; logicalIndex < batch.size; logicalIndex++) {
      final int batchIndex = (selectedInUse ? selected[logicalIndex] : logicalIndex);
      resultVectorExtractRow.extractRow(batch, batchIndex, scrqtchRow);

      Object copyResult =
          ObjectInspectorUtils.copyToStandardObject(
              scrqtchRow[0], objectInspector, ObjectInspectorCopyOption.WRITABLE);
      resultObjects[rowIndex++] = copyResult;
    }
  }

  private void doVectorArithmeticTest(
      List<String> columns,
      String[] columnNames,
      TypeInfo[] typeInfos,
      ExprNodeGenericFuncDesc exprDesc,
      ArithmeticTestMode arithmeticTestMode,
      VectorRandomBatchSource batchSource,
      ObjectInspector objectInspector,
      TypeInfo outputTypeInfo, Object[] resultObjects)
          throws Exception {

    HiveConf hiveConf = new HiveConf();
    if (arithmeticTestMode == ArithmeticTestMode.ADAPTOR) {
      hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_VECTOR_ADAPTOR_OVERRIDE, true);
    }

    VectorizationContext vectorizationContext =
        new VectorizationContext(
            "name",
            columns,
            hiveConf);

    VectorExpression vectorExpression = vectorizationContext.getVectorExpression(exprDesc);
    String[] outputScratchTypeNames= vectorizationContext.getScratchColumnTypeNames();

    VectorizedRowBatchCtx batchContext =
        new VectorizedRowBatchCtx(
            columnNames,
            typeInfos,
            /* dataColumnNums */ null,
            /* partitionColumnCount */ 0,
            outputScratchTypeNames);

    VectorizedRowBatch batch = batchContext.createVectorizedRowBatch();

    VectorExtractRow resultVectorExtractRow = new VectorExtractRow();
    resultVectorExtractRow.init(
        new TypeInfo[] { outputTypeInfo }, new int[] { vectorExpression.getOutputColumn() });
    Object[] scrqtchRow = new Object[1];

    batchSource.resetBatchIteration();
    int rowIndex = 0;

    while (batchSource.fillNextBatch(batch)) {
      vectorExpression.evaluate(batch);
      extractResultObjects(batch, rowIndex, resultVectorExtractRow, scrqtchRow,
              objectInspector, resultObjects);
      rowIndex += batch.size;
    }
  }
}
