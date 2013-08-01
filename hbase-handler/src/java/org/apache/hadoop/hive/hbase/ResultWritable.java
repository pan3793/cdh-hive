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

package org.apache.hadoop.hive.hbase;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

public class ResultWritable implements Writable {
  private static final byte RESULT_VERSION = (byte)0;

  private Result result;

  public ResultWritable() {

  }
  public ResultWritable(Result result) {
    this.result = result;
  }

  public Result getResult() {
    return result;
  }
  public void setResult(Result result) {
    this.result = result;
  }
  //Writable
  public void readFields(final DataInput in)
  throws IOException {
    int version = in.readByte();
    if (version > RESULT_VERSION) {
      throw new IOException("version not supported");
    }
    int size = in.readInt();
    if(size < 0) {
      throw new IOException("Invalid size " + size);
    }
    KeyValue[] kvs = new KeyValue[size];
    int totalBuffer = in.readInt();
    if(totalBuffer > 0) {
      byte [] raw = new byte[totalBuffer];
      DataInputStream kvIn = new DataInputStream(new ByteArrayInputStream(raw));
      for (int i = 0; i < kvs.length; i++) {
        int kvLength = kvIn.readInt();
        if(kvLength < 0) {
          throw new IOException("Invalid length " + kvLength);
        }
        byte[] kvBuff = new byte[kvLength];
        kvIn.readFully(kvBuff);
        kvs[i] = new KeyValue(kvBuff);
      }
    }
    result = new Result(kvs);
  }

  public void write(final DataOutput out)
  throws IOException {
    out.writeByte(RESULT_VERSION);
    if(result.isEmpty()) {
      out.writeInt(0);
    } else {
      out.writeInt(result.size());
      int totalLen = 0;
      for(KeyValue kv : result.list()) {
        totalLen += kv.getLength() + Bytes.SIZEOF_INT;
      }
      out.writeInt(totalLen);
      for(KeyValue kv : result.list()) {
        out.writeInt(kv.getLength());
        out.write(kv.getBuffer(), kv.getOffset(), kv.getLength());
      }
    }
  }

}
