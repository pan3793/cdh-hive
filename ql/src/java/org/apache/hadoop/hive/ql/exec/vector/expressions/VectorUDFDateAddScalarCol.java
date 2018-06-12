/**
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

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hive.common.util.DateParser;

import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;


public class VectorUDFDateAddScalarCol extends VectorExpression {
  private static final long serialVersionUID = 1L;

  private int colNum;
  private int outputColumn;
  private Object object;
  private long longValue = 0;
  private Timestamp timestampValue = null;
  private byte[] stringValue = null;
  protected boolean isPositive = true;
  private transient final DateParser dateParser = new DateParser();
  private transient final Date baseDate = new Date(0);

  public VectorUDFDateAddScalarCol() {
    super();
  }

  public VectorUDFDateAddScalarCol(Object object, int colNum, int outputColumn) {
    this();
    this.colNum = colNum;
    this.outputColumn = outputColumn;

    this.object = object;
    if (object instanceof Long) {
      this.longValue = (Long) object;
    } else if (object instanceof Timestamp) {
        this.timestampValue = (Timestamp) object;
    } else if (object instanceof byte []) {
      this.stringValue = (byte[]) object;
    } else {
      throw new RuntimeException("Unexpected scalar object " + object.getClass().getName() + " " + object.toString());
    }
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    LongColumnVector inputCol = (LongColumnVector) batch.cols[this.colNum];
    /* every line below this is identical for evaluateLong & evaluateString */
    final int n = inputCol.isRepeating ? 1 : batch.size;
    int[] sel = batch.selected;
    final boolean selectedInUse = (inputCol.isRepeating == false) && batch.selectedInUse;
    LongColumnVector outputColVector = (LongColumnVector) batch.cols[outputColumn];
    boolean[] outputIsNull = outputColVector.isNull;

    switch (inputTypes[0]) {
      case DATE:
        baseDate.setTime(DateWritable.daysToMillis((int) longValue));
        break;

      case TIMESTAMP:
        baseDate.setTime(timestampValue.getTime());
        break;

      case STRING:
      case CHAR:
      case VARCHAR:
        boolean parsed = dateParser.parseDate(new String(stringValue, StandardCharsets.UTF_8), baseDate);
        if (!parsed) {
          outputColVector.noNulls = false;
          if (selectedInUse) {
            for(int j=0; j < n; j++) {
              int i = sel[j];
              outputColVector.isNull[i] = true;
            }
          } else {
            for(int i = 0; i < n; i++) {
              outputColVector.isNull[i] = true;
            }
          }
          return;
        }
        break;
      default:
        throw new Error("Unsupported input type " + inputTypes[0].name());
    }

    if(batch.size == 0) {
      /* n != batch.size when isRepeating */
      return;
    }

    // We do not need to do a column reset since we are carefully changing the output.
    outputColVector.isRepeating = false;

    long baseDateDays = DateWritable.millisToDays(baseDate.getTime());
    if (inputCol.isRepeating) {
      if (inputCol.noNulls || !inputCol.isNull[0]) {
        outputColVector.isNull[0] = false;
        evaluate(baseDateDays, inputCol.vector[0], outputColVector, 0);
      } else {
        outputColVector.isNull[0] = true;
        outputColVector.noNulls = false;
      }
      outputColVector.isRepeating = true;
      return;
    }

    if (inputCol.noNulls) {
      if (batch.selectedInUse) {

        // CONSIDER: For large n, fill n or all of isNull array and use the tighter ELSE loop.

        if (!outputColVector.noNulls) {
          for(int j = 0; j != n; j++) {
           final int i = sel[j];
           // Set isNull before call in case it changes it mind.
           outputIsNull[i] = false;
           evaluate(baseDateDays, inputCol.vector[i], outputColVector, i);
         }
        } else {
          for(int j = 0; j != n; j++) {
            final int i = sel[j];
            evaluate(baseDateDays, inputCol.vector[i], outputColVector, i);
          }
        }
      } else {
        if (!outputColVector.noNulls) {

          // Assume it is almost always a performance win to fill all of isNull so we can
          // safely reset noNulls.
          Arrays.fill(outputIsNull, false);
          outputColVector.noNulls = true;
        }
        for(int i = 0; i != n; i++) {
          evaluate(baseDateDays, inputCol.vector[i], outputColVector, i);
        }
      }
    } else /* there are nulls in the inputColVector */ {

      // Carefully handle NULLs..

      // Handle case with nulls. Don't do function if the value is null, to save time,
      // because calling the function can be expensive.
      outputColVector.noNulls = false;

      if (selectedInUse) {
        for(int j = 0; j < n; j++) {
          int i = sel[j];
          outputColVector.isNull[i] = inputCol.isNull[i];
          if (!inputCol.isNull[i]) {
            evaluate(baseDateDays, inputCol.vector[i], outputColVector, i);
          }
        }
      } else {
        for(int i = 0; i < n; i++) {
          outputColVector.isNull[i] = inputCol.isNull[i];
          if (!inputCol.isNull[i]) {
            evaluate(baseDateDays, inputCol.vector[i], outputColVector, i);
          }
        }
      }
    }
  }

  private void evaluate(long baseDateDays, long numDays, LongColumnVector output, int i) {
    long result = baseDateDays;
    if (isPositive) {
      result += numDays;
    } else {
      result -= numDays;
    }
    output.vector[i] = result;
  }

  @Override
  public int getOutputColumn() {
    return this.outputColumn;
  }

  public int getColNum() {
    return colNum;
  }

  public void setColNum(int colNum) {
    this.colNum = colNum;
  }

  public void setOutputColumn(int outputColumn) {
    this.outputColumn = outputColumn;
  }

  public long getLongValue() {
    return longValue;
  }

  public void setLongValue(long longValue) {
    this.longValue = longValue;
  }

  public byte[] getStringValue() {
    return stringValue;
  }

  public void setStringValue(byte[] stringValue) {
    this.stringValue = stringValue;
  }

  public boolean isPositive() {
    return isPositive;
  }

  public void setPositive(boolean isPositive) {
    this.isPositive = isPositive;
  }

  @Override
  public String vectorExpressionParameters() {
    String value;
    if (object instanceof Long) {
      Date tempDate = new Date(0);
      tempDate.setTime(DateWritable.daysToMillis((int) longValue));
      value = tempDate.toString();
    } else if (object instanceof Timestamp) {
      value = this.timestampValue.toString();
    } else if (object instanceof byte []) {
      value = new String(this.stringValue, StandardCharsets.UTF_8);
    } else {
      value = "unknown";
    }
    return "val " + value + ", " + getColumnParamString(0, colNum);
  }

  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.STRING_DATETIME_FAMILY,
            VectorExpressionDescriptor.ArgumentType.INT_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.SCALAR,
            VectorExpressionDescriptor.InputExpressionType.COLUMN);
    return b.build();
  }
}
