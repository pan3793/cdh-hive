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

package org.apache.hadoop.hive.ql.exec.vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriterFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.spark.SparkPartitionPruningSinkDesc;
import org.apache.hadoop.hive.ql.parse.spark.SparkPartitionPruningSinkOperator;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;


/**
 * Vectorized version for SparkPartitionPruningSinkOperator.
 * Forked from VectorAppMasterEventOperator.
 *
 * The vectorization APIs between CDH and Apache Hive are very different, so backporting this class required rewriting
 * a lot of it. Most of the rewrite is copied from {@link VectorAppMasterEventOperator}.
 **/
public class VectorSparkPartitionPruningSinkOperator extends SparkPartitionPruningSinkOperator {

  private static final long serialVersionUID = 1L;

  private VectorizationContext vContext;

  protected transient VectorExpressionWriter[] valueWriters;

  protected transient Object[] singleRow;

  public VectorSparkPartitionPruningSinkOperator(VectorizationContext context,
      OperatorDesc conf) {
    super();
    this.conf = (SparkPartitionPruningSinkDesc) conf;
    this.vContext = context;
  }

  public VectorSparkPartitionPruningSinkOperator() {
  }

  @Override
  public void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
    valueWriters = VectorExpressionWriterFactory.getExpressionWriters(
        (StructObjectInspector) inputObjInspectors[0]);
    singleRow = new Object[valueWriters.length];
  }

  @Override
  public void processOp(Object data, int tag) throws HiveException {

    VectorizedRowBatch vrg = (VectorizedRowBatch) data;

    Writable [] records = null;
    Writable recordValue = null;
    boolean vectorizedSerde = false;

    try {
      if (serializer instanceof VectorizedSerde) {
        recordValue = ((VectorizedSerde) serializer).serializeVector(vrg,
            inputObjInspectors[0]);
        records = (Writable[]) ((ObjectWritable) recordValue).get();
        vectorizedSerde = true;
      }
    } catch (SerDeException e1) {
      throw new HiveException(e1);
    }

    for (int i = 0; i < vrg.size; i++) {
      Writable row = null;
      if (vectorizedSerde) {
        row = records[i];
      } else {
        if (vrg.valueWriters == null) {
          vrg.setValueWriters(this.valueWriters);
        }
        try {
          row = serializer.serialize(getRowObject(vrg, i), inputObjInspectors[0]);
        } catch (SerDeException ex) {
          throw new HiveException(ex);
        }
      }
      try {
        row.write(buffer);
      } catch (Exception e) {
        throw new HiveException(e);
      }
    }
  }

  private Object[] getRowObject(VectorizedRowBatch vrg, int rowIndex)
          throws HiveException {
    int batchIndex = rowIndex;
    if (vrg.selectedInUse) {
      batchIndex = vrg.selected[rowIndex];
    }
    for (int i = 0; i < vrg.projectionSize; i++) {
      ColumnVector vectorColumn = vrg.cols[vrg.projectedColumns[i]];
      singleRow[i] = vrg.valueWriters[i].writeValue(vectorColumn, batchIndex);
    }
    return singleRow;
  }
}
