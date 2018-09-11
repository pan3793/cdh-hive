/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore.messaging.json;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.messaging.AddPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterIndexMessage;
import org.apache.hadoop.hive.metastore.messaging.CreateFunctionMessage;
import org.apache.hadoop.hive.metastore.messaging.CreateIndexMessage;
import org.apache.hadoop.hive.metastore.messaging.DropFunctionMessage;
import org.apache.hadoop.hive.metastore.messaging.DropIndexMessage;
import org.apache.hadoop.hive.metastore.messaging.DropPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.InsertMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ExtendedJSONMessageFactory extends MessageFactory {
  private static final Log LOG = LogFactory.getLog(ExtendedJSONMessageFactory.class.getName());
  private static ExtendedJSONMessageDeserializer deserializer = new ExtendedJSONMessageDeserializer();
  protected static final HiveConf hiveConf = new HiveConf();

  static {
    hiveConf.addResource("hive-site.xml");
  }

  protected static final String MS_SERVER_URL = hiveConf.get(HiveConf.ConfVars.METASTOREURIS.name(), "");
  protected static final String MS_SERVICE_PRINCIPAL =
      hiveConf.get(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.name(), "");
  // This class has basic information from a Partition. It is used to get a list of Partition
  // information from an iterator instead of having a list of Partition objects. Partition objects
  // may hold more information that can cause memory issues if we get a large list.
  private class PartitionBasicInfo {
    private List<Map<String, String>> partitionList = Lists.newArrayList();
    private List<String> locations = Lists.newArrayList();
    private List<Partition> partitions = Lists.newArrayList();

    public List<Map<String, String>> getPartitionList() {
      return partitionList;
    }

    public List<Partition> getPartitions() {
      return partitions;
    }

    public List<String> getLocations() {
      return locations;
    }
  }

  public ExtendedJSONMessageFactory() {
    LOG.info("Using ExtendedJSONMessageFactory for building Notification log messages ");
  }

  public MessageDeserializer getDeserializer() {
    return deserializer;
  }

  public String getVersion() {
    return "0.1";
  }

  public String getMessageFormat() {
    return "json";
  }

  public ExtendedJSONCreateDatabaseMessage buildCreateDatabaseMessage(Database db) {
    return new ExtendedJSONCreateDatabaseMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, db.getName(),
        now(), db.getLocationUri());
  }

  public ExtendedJSONAlterDatabaseMessage buildAlterDatabaseMessage(Database before, Database after) {
    return new ExtendedJSONAlterDatabaseMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, before,
      after, now());
  }

  public ExtendedJSONDropDatabaseMessage buildDropDatabaseMessage(Database db) {
    return new ExtendedJSONDropDatabaseMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, db.getName(),
        now(), db.getLocationUri());
  }

  public ExtendedJSONCreateTableMessage buildCreateTableMessage(Table table) {
    String location = null;
    if (table != null && table.isSetSd()) {
      location = table.getSd().getLocation();
    }
    return new ExtendedJSONCreateTableMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, table, now(), location);
  }

  public ExtendedJSONAlterTableMessage buildAlterTableMessage(Table before, Table after) {
    String oldLocation = null;
    String newLocation = null;

    if(before != null && before.isSetSd()) {
      oldLocation = before.getSd().getLocation();
    }

    if(after != null && after.isSetSd()) {
      newLocation = after.getSd().getLocation();
    }

    return new ExtendedJSONAlterTableMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, before, after, now(), oldLocation,
        newLocation);
  }

  public ExtendedJSONDropTableMessage buildDropTableMessage(Table table) {
    String location = null;
    if (table != null && table.isSetSd()) {
      location = table.getSd().getLocation();
    }
    return new ExtendedJSONDropTableMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, table.getDbName(),
        table.getTableName(), now(), location);
  }

  @Override
  public ExtendedJSONAlterPartitionMessage buildAlterPartitionMessage(Table table,
      Partition before, Partition after) {
    String oldLocation = null;
    String newLocation = null;
    if (before != null && before.isSetSd()) {
      oldLocation = before.getSd().getLocation();
    }
    if (after != null && after.isSetSd()) {
      newLocation = after.getSd().getLocation();
    }
    return new ExtendedJSONAlterPartitionMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, table, before, after, now(),
        after.getValues(), oldLocation, newLocation);
  }

  @Override
  public DropPartitionMessage buildDropPartitionMessage(Table table, Iterator<Partition> partitions) {
    PartitionBasicInfo partitionBasicInfo = getPartitionBasicInfo(table, partitions);

    return new ExtendedJSONDropPartitionMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL,
        table.getDbName(), table.getTableName(), partitionBasicInfo.getPartitionList(),
        now(), partitionBasicInfo.getLocations());
  }

  @Override
  public CreateFunctionMessage buildCreateFunctionMessage(Function function) {
    // Sentry would be not be interested in CreateFunctionMessage as these are generated when is data is
    // added inserted. This method is implemented for completeness. This is reason why, new sentry
    // JSON class is not defined for CreateFunctionMessage
    return new JSONCreateFunctionMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, function, now());
  }

  @Override
  public DropFunctionMessage buildDropFunctionMessage(Function function) {
    // Sentry would be not be interested in DropFunctionMessage as these are generated when is data is
    // added inserted. This method is implemented for completeness. This is reason why, new sentry
    // JSON class is not defined for DropFunctionMessage
    return new JSONDropFunctionMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, function, now());

  }

  @Override
  public CreateIndexMessage buildCreateIndexMessage(Index index) {
    // Sentry would be not be interested in CreateIndexMessage as these are generated when is data is
    // added inserted. This method is implemented for completeness. This is reason why, new sentry
    // JSON class is not defined for CreateIndexMessage
    return new JSONCreateIndexMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, index, now());
  }

  @Override
  public DropIndexMessage buildDropIndexMessage(Index index) {
    // Sentry would be not be interested in DropIndexMessage as these are generated when is data is
    // added inserted. This method is implemented for completeness. This is reason why, new sentry
    // JSON class is not defined for DropIndexMessage
    return new JSONDropIndexMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, index, now());
  }

  @Override
  public AlterIndexMessage buildAlterIndexMessage(Index before, Index after) {
    // Sentry would be not be interested in AlterIndexMessage as these are generated when is data is
    // added inserted. This method is implemented for completeness. This is reason why, new sentry
    // JSON class is not defined for AlterIndexMessage
    return new JSONAlterIndexMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, before, after, now());
  }

  @Override
  public InsertMessage buildInsertMessage(String db, String table, Map<String,String> partKeyVals,
      List<String> files) {
    // Sentry would be not be interested in InsertMessage as these are generated when is data is
    // added inserted. This method is implemented for completeness. This is reason why, new sentry
    // JSON class is not defined for InsertMessage.
    return new JSONInsertMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, db, table, partKeyVals,
        files, now());
  }

  @Override
  public AddPartitionMessage buildAddPartitionMessage(Table table,
      Iterator<Partition> partitionsIterator) {
    PartitionBasicInfo partitionBasicInfo = getPartitionBasicInfo(table, partitionsIterator);

    return new ExtendedJSONAddPartitionMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, table,
        partitionBasicInfo.getPartitions(), now(), partitionBasicInfo.getLocations());
  }

  public AddPartitionMessage buildAddPartitionMessage(Table table,
                                                      List<Partition> partitions) {
    return buildAddPartitionMessage (table, partitions.iterator());
  }

  private PartitionBasicInfo getPartitionBasicInfo(Table table, Iterator<Partition> iterator) {
    PartitionBasicInfo partitionBasicInfo = new PartitionBasicInfo();
    while(iterator.hasNext()) {
      Partition partition = iterator.next();
      partitionBasicInfo.getPartitions().add(partition);
      partitionBasicInfo.getPartitionList().add(getPartitionKeyValues(table, partition));
      partitionBasicInfo.getLocations().add(partition.getSd().getLocation());
    }

    return partitionBasicInfo;
  }

  private static Map<String, String> getPartitionKeyValues(Table table, Partition partition) {
    LinkedHashMap partitionKeys = new LinkedHashMap();
    if (table.getPartitionKeysSize() != partition.getValuesSize()) {
      LOG.error("PartitionKeys size and partition values size different.");
      return partitionKeys;
    }
    for (int i = 0; i < table.getPartitionKeysSize(); ++i) {
      partitionKeys.put((table.getPartitionKeys().get(i)).getName(), partition.getValues().get(i));
    }

    return partitionKeys;
  }

  //This is private in parent class
  private long now() {
    return System.currentTimeMillis() / 1000L;
  }
}
