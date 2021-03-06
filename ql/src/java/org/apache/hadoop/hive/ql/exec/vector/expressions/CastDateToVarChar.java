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

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class CastDateToVarChar extends CastDateToString implements TruncStringOutput {
  private static final long serialVersionUID = 1L;
  private int maxLength; // Must be manually set with setMaxLength.

  public CastDateToVarChar() {
    super();
  }

  public CastDateToVarChar(int inputColumn, int outputColumn) {
    super(inputColumn, outputColumn);
  }

  @Override
  protected void assign(BytesColumnVector outV, int i, byte[] bytes, int length) {
    StringExpr.truncate(outV, i, bytes, 0, length, maxLength);
  }

  @Override
  public int getMaxLength() {
    return maxLength;
  }

  @Override
  public void setMaxLength(int maxLength) {
    this.maxLength = maxLength;
  }

  @Override
  public String vectorExpressionParameters() {
    return "col " + inputColumn + ", maxLength " + maxLength;
  }
}
