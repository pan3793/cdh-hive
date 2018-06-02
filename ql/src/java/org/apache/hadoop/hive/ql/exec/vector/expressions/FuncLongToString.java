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

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * Superclass to support vectorized functions that take a long
 * and return a string, optionally with additional configuraiton arguments.
 * Used for bin(long), hex(long) etc.
 */
public abstract class FuncLongToString extends VectorExpression {
  private static final long serialVersionUID = 1L;

  private int inputCol;
  private int outputCol;
  protected transient byte[] bytes;

  FuncLongToString(int inputCol, int outputCol) {
    this.inputCol = inputCol;
    this.outputCol = outputCol;
    bytes = new byte[64];    // staging area for results, to avoid new() calls
  }

  FuncLongToString() {
    bytes = new byte[64];
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    LongColumnVector inputColVector = (LongColumnVector) batch.cols[inputCol];
    int[] sel = batch.selected;
    int n = batch.size;
    long[] vector = inputColVector.vector;
    BytesColumnVector outputColVector = (BytesColumnVector) batch.cols[outputCol];
    outputColVector.initBuffer();

    boolean[] inputIsNull = inputColVector.isNull;
    boolean[] outputIsNull = outputColVector.isNull;

    if (n == 0) {
      //Nothing to do
      return;
    }

    // We do not need to do a column reset since we are carefully changing the output.
    outputColVector.isRepeating = false;

    if (inputColVector.isRepeating) {
      if (inputColVector.noNulls || !inputIsNull[0]) {
        // Set isNull before call in case it changes it mind.
        outputIsNull[0] = false;
        prepareResult(0, vector, outputColVector);
      } else {
        outputIsNull[0] = true;
        outputColVector.noNulls = false;
      }
      outputColVector.isRepeating = true;
      return;
    }

    if (inputColVector.noNulls) {
      if (batch.selectedInUse) {

        // CONSIDER: For large n, fill n or all of isNull array and use the tighter ELSE loop.

        if (!outputColVector.noNulls) {
          for(int j = 0; j != n; j++) {
           final int i = sel[j];
           // Set isNull before call in case it changes it mind.
           outputIsNull[i] = false;
           prepareResult(i, vector, outputColVector);
         }
        } else {
          for(int j = 0; j != n; j++) {
            final int i = sel[j];
            prepareResult(i, vector, outputColVector);
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
          prepareResult(i, vector, outputColVector);
        }
      }
    } else /* there are nulls in the inputColVector */ {

      // Carefully handle NULLs...
      outputColVector.noNulls = false;

      if (batch.selectedInUse) {
        for(int j=0; j != n; j++) {
          int i = sel[j];
          outputColVector.isNull[i] = inputColVector.isNull[i];
          if (!inputColVector.isNull[i]) {
            prepareResult(i, vector, outputColVector);
          }
        }
      } else {
        for(int i = 0; i != n; i++) {
          outputColVector.isNull[i] = inputColVector.isNull[i];
          if (!inputColVector.isNull[i]) {
            prepareResult(i, vector, outputColVector);
          }
        }
      }
    }
  }

  /* Evaluate result for position i (using bytes[] to avoid storage allocation costs)
   * and set position i of the output vector to the result.
   */
  abstract void prepareResult(int i, long[] vector, BytesColumnVector outputColVector);

  @Override
  public int getOutputColumn() {
    return outputCol;
  }

  public int getOutputCol() {
    return outputCol;
  }

  public void setOutputCol(int outputCol) {
    this.outputCol = outputCol;
  }

  public int getInputCol() {
    return inputCol;
  }

  public void setInputCol(int inputCol) {
    this.inputCol = inputCol;
  }

  @Override
  public String vectorExpressionParameters() {
    return "col " + inputCol;
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder()).setMode(
        VectorExpressionDescriptor.Mode.PROJECTION).setNumArguments(1).setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN).setArgumentTypes(
                VectorExpressionDescriptor.ArgumentType.INT_FAMILY).build();
  }
}
