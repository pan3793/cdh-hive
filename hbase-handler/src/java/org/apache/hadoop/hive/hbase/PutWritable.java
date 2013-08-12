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
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class PutWritable implements Writable {
  private static final byte PUT_VERSION = (byte)0;

  private Put put;

  public PutWritable() {

  }
  public PutWritable(Put put) {
    this.put = put;
  }
  public Put getPut() {
    return put;
  }
  //Writable
  public void readFields(final DataInput in)
  throws IOException {
    int version = in.readByte();
    if (version > PUT_VERSION) {
      throw new IOException("version not supported");
    }
    byte[] row = Bytes.readByteArray(in);
    long ts = in.readLong();
    Durability durability = Durability.valueOf(in.readUTF());
    int numFamilies = in.readInt();
    NavigableMap<byte[],List<? extends Cell>> familyMap = new TreeMap<byte[],List<? extends Cell>>();
    for(int i = 0; i < numFamilies; i++) {
      byte [] family = Bytes.readByteArray(in);
      int numKeys = in.readInt();
      List<KeyValue> keys = new ArrayList<KeyValue>(numKeys);
      int totalLen = in.readInt();
      byte [] buf = new byte[totalLen];
      int offset = 0;
      for (int j = 0; j < numKeys; j++) {
        int keyLength = in.readInt();
        in.readFully(buf, offset, keyLength);
        keys.add(new KeyValue(buf, offset, keyLength));
        offset += keyLength;
      }
      familyMap.put(family, keys);
    }
    put = new Put(row, ts);
    put.setFamilyMap(familyMap);
    put.setDurability(durability);
    readAttributes(in);
  }

  public void write(final DataOutput out)
  throws IOException {
    out.writeByte(PUT_VERSION);
    Bytes.writeByteArray(out, put.getRow());
    out.writeLong(put.getTimeStamp());
    Durability durabilty = put.getDurability();
    if(durabilty == null) {
      durabilty = Durability.USE_DEFAULT;
    }
    out.writeUTF(durabilty.name());
    NavigableMap<byte[],List<? extends Cell>> familyMap = put.getFamilyCellMap();
    out.writeInt(familyMap.size());
    for (Map.Entry<byte [], List<? extends Cell>> entry : familyMap.entrySet()) {
      Bytes.writeByteArray(out, entry.getKey());
      List<KeyValue> keys = (List<KeyValue>)entry.getValue();
      out.writeInt(keys.size());
      int totalLen = 0;
      for(KeyValue kv : keys) {
        totalLen += kv.getLength();
      }
      out.writeInt(totalLen);
      for(KeyValue kv : keys) {
        out.writeInt(kv.getLength());
        out.write(kv.getBuffer(), kv.getOffset(), kv.getLength());
      }
    }
    writeAttributes(out);
  }

  private void writeAttributes(final DataOutput out) throws IOException {
    Map<String, byte[]> attributes = put.getAttributesMap();
    if (attributes == null) {
      out.writeInt(0);
    } else {
      out.writeInt(attributes.size());
      for (Map.Entry<String, byte[]> attr : attributes.entrySet()) {
        WritableUtils.writeString(out, attr.getKey());
        Bytes.writeByteArray(out, attr.getValue());
      }
    }
  }

  private void readAttributes(final DataInput in) throws IOException {
    int numAttributes = in.readInt();
    if (numAttributes > 0) {
      Map<String, byte[]> attributes = new HashMap<String, byte[]>(numAttributes);
      for(int i=0; i<numAttributes; i++) {
        String name = WritableUtils.readString(in);
        byte[] value = Bytes.readByteArray(in);
        attributes.put(name, value);
      }
    }
  }
}
