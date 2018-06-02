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

import java.util.Arrays;
import java.util.Calendar;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hive.common.util.DateUtils;

import com.google.common.base.Preconditions;


/**
 * Abstract class to return various fields from a Timestamp or Date.
 */
public abstract class VectorUDFTimestampFieldDate extends VectorExpression {

  private static final long serialVersionUID = 1L;

  protected int colNum;
  protected int outputColumn;
  protected int field;
  protected transient final Calendar calendar = Calendar.getInstance();

  public VectorUDFTimestampFieldDate(int field, int colNum, int outputColumn) {
    this();
    this.colNum = colNum;
    this.outputColumn = outputColumn;
    this.field = field;
  }

  public VectorUDFTimestampFieldDate() {
    super();
  }

  protected long getDateField(long days) {
    calendar.setTimeInMillis(DateWritable.daysToMillis((int) days));
    return calendar.get(field);
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {

    Preconditions.checkState(inputTypes[0] == VectorExpression.Type.DATE);

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    LongColumnVector outputColVector = (LongColumnVector) batch.cols[outputColumn];
    ColumnVector inputColVec = batch.cols[this.colNum];

    /* every line below this is identical for evaluateLong & evaluateString */
    final int n = inputColVec.isRepeating ? 1 : batch.size;
    int[] sel = batch.selected;
    final boolean selectedInUse = (inputColVec.isRepeating == false) && batch.selectedInUse;
    boolean[] outputIsNull = outputColVector.isNull;

    if(batch.size == 0) {
      /* n != batch.size when isRepeating */
      return;
    }

    // We do not need to do a column reset since we are carefully changing the output.
    outputColVector.isRepeating = false;

    LongColumnVector longColVector = (LongColumnVector) inputColVec;

    if (inputColVec.isRepeating) {
      if (inputColVec.noNulls || !inputColVec.isNull[0]) {
        outputColVector.isNull[0] = false;
        outputColVector.vector[0] = getDateField(longColVector.vector[0]);
      } else {
        outputColVector.isNull[0] = true;
        outputColVector.noNulls = false;
      }
      outputColVector.isRepeating = true;
      return;
    }

    if (inputColVec.noNulls) {
      if (batch.selectedInUse) {

        // CONSIDER: For large n, fill n or all of isNull array and use the tighter ELSE loop.

        if (!outputColVector.noNulls) {
          for(int j = 0; j != n; j++) {
           final int i = sel[j];
           // Set isNull before call in case it changes it mind.
           outputIsNull[i] = false;
           outputColVector.vector[i] = getDateField(longColVector.vector[i]);
         }
        } else {
          for(int j = 0; j != n; j++) {
            final int i = sel[j];
            outputColVector.vector[i] = getDateField(longColVector.vector[i]);
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
          outputColVector.vector[i] = getDateField(longColVector.vector[i]);
        }
      }
    } else /* there are nulls in the inputColVector */ {

      // Carefully handle NULLs...
      outputColVector.noNulls = false;

      if (selectedInUse) {
        for(int j=0; j < n; j++) {
          int i = sel[j];
          outputColVector.isNull[i] = inputColVec.isNull[i];
          if (!inputColVec.isNull[i]) {
            outputColVector.vector[i] = getDateField(longColVector.vector[i]);
          }
        }
      } else {
        for(int i = 0; i < n; i++) {
          outputColVector.isNull[i] = inputColVec.isNull[i];
          if (!inputColVec.isNull[i]) {
            outputColVector.vector[i] = getDateField(longColVector.vector[i]);
          }
        }
      }
    }
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

  public int getField() {
    return field;
  }

  public void setField(int field) {
    this.field = field;
  }

  public void setOutputColumn(int outputColumn) {
    this.outputColumn = outputColumn;
  }

  @Override
  public String vectorExpressionParameters() {
    if (field == -1) {
      return "col " + colNum;
    } else {
      return "col " + colNum + ", field " + DateUtils.getFieldName(field);
    }
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(1)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.DATE)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN);
    return b.build();
  }
}
