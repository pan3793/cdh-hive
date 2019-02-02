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

package org.apache.hadoop.hive.metastore.events;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InsertEventRequestData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class InsertEvent extends ListenerEvent {

  private final Table tableObj;
  private final Partition ptnObj;
  private final boolean replace;
  private final List<String> files;

  /**
   *
   * @param db name of the database the table is in
   * @param table name of the table being inserted into
   * @param partVals list of partition values, can be null
   * @param status status of insert, true = success, false = failure
   * @param handler handler that is firing the event
   */
  public InsertEvent(String db, String table, List<String> partVals, InsertEventRequestData insertData,
                     boolean status, HMSHandler handler) throws MetaException, NoSuchObjectException {
    super(status, handler);
    this.replace = (insertData.isSetReplace() ? insertData.isReplace() : true);
    this.files = insertData.getFilesAdded();
    try {
      this.tableObj = handler.get_table(db, table);
      if (partVals != null) {
        this.ptnObj = handler.get_partition(db, table, partVals);
      } else {
        this.ptnObj = null;
      }
    } catch (NoSuchObjectException e) {
      // This is to mimic previous behavior where NoSuchObjectException was thrown through this
      // method.
      throw e;
    } catch (TException e) {
      throw MetaStoreUtils.newMetaException(e);
    }


  }

  /**
   * @return Table object
   */
  public Table getTableObj() {
    return tableObj;
  }

  /**
   * @return Partition object
   */
  public Partition getPartitionObj() {
    return ptnObj;
  }

  /**
   * @return The replace flag.
   */
  public boolean isReplace() {
    return replace;
  }

  /**
   * Get list of files created as a result of this DML operation
   * @return list of new files
   */
  public List<String> getFiles() {
    return files;
  }
}
