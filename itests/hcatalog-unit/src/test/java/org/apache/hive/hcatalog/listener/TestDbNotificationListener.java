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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
*/
package org.apache.hive.hcatalog.listener;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import com.google.common.collect.Lists;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.MetaStoreEventListenerConstants;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventRequestData;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InsertEventRequestData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.messaging.CreateDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.CreateTableMessage;
import org.apache.hadoop.hive.metastore.messaging.DropDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.AddPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterTableMessage;
import org.apache.hadoop.hive.metastore.messaging.DropPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.DropTableMessage;
import org.apache.hadoop.hive.metastore.messaging.InsertMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hadoop.hive.metastore.events.AlterDatabaseEvent;
import org.apache.hadoop.hive.metastore.messaging.json.JSONMessageFactory;
import org.apache.hadoop.hive.metastore.events.AddIndexEvent;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterIndexEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateFunctionEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropFunctionEvent;
import org.apache.hadoop.hive.metastore.events.DropIndexEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.InsertEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage.EventType;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.data.Pair;
import org.junit.After;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TJSONProtocol;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestDbNotificationListener {
  private static final Logger LOG = LoggerFactory.getLogger(TestDbNotificationListener.class.getName());
  private static final int EVENTS_TTL = 30;
  private static final int CLEANUP_SLEEP_TIME = 10;
  private static Map<String, String> emptyParameters = new HashMap<String, String>();
  private static IMetaStoreClient msClient;
  private static IDriver driver;
  private static MessageDeserializer md = null;
  private int startTime;
  private long firstEventId;
  private final String testTempDir = Paths.get(System.getProperty("java.io.tmpdir"), "testDbNotif").toString();

  // The HiveConf used here is static, hence any config changed in a test should be explicitly reset to
  // default value at the end of the test so that it will not affect subsequent tests.
  private static HiveConf conf;

  /* This class is used to verify that HiveMetaStore calls the non-transactional listeners with the
    * current event ID set by the DbNotificationListener class */
  public static class MockMetaStoreEventListener extends MetaStoreEventListener {
    private static Stack<Pair<EventType, String>> eventsIds = new Stack<>();

    private static void pushEventId(EventType eventType, final ListenerEvent event) {
      if (event.getStatus()) {
        Map<String, String> parameters = event.getParameters();
        if (parameters.containsKey(MetaStoreEventListenerConstants.DB_NOTIFICATION_EVENT_ID_KEY_NAME)) {
          Pair<EventType, String> pair =
              new Pair<>(eventType, parameters.get(MetaStoreEventListenerConstants.DB_NOTIFICATION_EVENT_ID_KEY_NAME));
          eventsIds.push(pair);
        }
      }
    }

    public static void popAndVerifyLastEventId(EventType eventType, long id) {
      if (!eventsIds.isEmpty()) {
        Pair<EventType, String> pair = eventsIds.pop();

        assertEquals("Last event type does not match.", eventType, pair.first);
        assertEquals("Last event ID does not match.", Long.toString(id), pair.second);
      } else {
        assertTrue("List of events is empty.",false);
      }
    }

    public static void clearEvents() {
      eventsIds.clear();
    }

    public MockMetaStoreEventListener(Configuration config) {
      super(config);
    }

    public void onCreateTable (CreateTableEvent tableEvent) throws MetaException {
      pushEventId(EventType.CREATE_TABLE, tableEvent);
    }

    public void onDropTable (DropTableEvent tableEvent)  throws MetaException {
      pushEventId(EventType.DROP_TABLE, tableEvent);
    }

    public void onAlterTable (AlterTableEvent tableEvent) throws MetaException {
      pushEventId(EventType.ALTER_TABLE, tableEvent);
    }

    public void onAddPartition (AddPartitionEvent partitionEvent) throws MetaException {
      pushEventId(EventType.ADD_PARTITION, partitionEvent);
    }

    public void onDropPartition (DropPartitionEvent partitionEvent)  throws MetaException {
      pushEventId(EventType.DROP_PARTITION, partitionEvent);
    }

    public void onAlterPartition (AlterPartitionEvent partitionEvent)  throws MetaException {
      pushEventId(EventType.ALTER_PARTITION, partitionEvent);
    }

    public void onCreateDatabase (CreateDatabaseEvent dbEvent) throws MetaException {
      pushEventId(EventType.CREATE_DATABASE, dbEvent);
    }

    public void onDropDatabase (DropDatabaseEvent dbEvent) throws MetaException {
      pushEventId(EventType.DROP_DATABASE, dbEvent);
    }

    public void onAlterDatabase (AlterDatabaseEvent dbEvent) throws MetaException {
      pushEventId(EventType.ALTER_DATABASE, dbEvent);
    }

    public void onAddIndex(AddIndexEvent indexEvent) throws MetaException {
      pushEventId(EventType.CREATE_INDEX, indexEvent);
    }

    public void onDropIndex(DropIndexEvent indexEvent) throws MetaException {
      pushEventId(EventType.DROP_INDEX, indexEvent);
    }

    public void onAlterIndex(AlterIndexEvent indexEvent) throws MetaException {
      pushEventId(EventType.ALTER_INDEX, indexEvent);
    }

    public void onCreateFunction (CreateFunctionEvent fnEvent) throws MetaException {
      pushEventId(EventType.CREATE_FUNCTION, fnEvent);
    }

    public void onDropFunction (DropFunctionEvent fnEvent) throws MetaException {
      pushEventId(EventType.DROP_FUNCTION, fnEvent);
    }

    public void onInsert(InsertEvent insertEvent) throws MetaException {
      pushEventId(EventType.INSERT, insertEvent);
    }
  }

  @SuppressWarnings("rawtypes")
  @BeforeClass
  public static void connectToMetastore() throws Exception {
    conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.METASTORE_TRANSACTIONAL_EVENT_LISTENERS,
        DbNotificationListener.class.getName());
    conf.setVar(HiveConf.ConfVars.METASTORE_EVENT_LISTENERS, MockMetaStoreEventListener.class.getName());
    conf.setVar(HiveConf.ConfVars.METASTORE_EVENT_DB_LISTENER_TTL, String.valueOf(EVENTS_TTL) + "s");
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setBoolVar(HiveConf.ConfVars.FIRE_EVENTS_FOR_DML, true);
    conf.setVar(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");
    conf.setVar(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL,
        DummyRawStoreFailEvent.class.getName());
    Class dbNotificationListener =
        Class.forName("org.apache.hive.hcatalog.listener.DbNotificationListener");
    Class[] classes = dbNotificationListener.getDeclaredClasses();
    for (Class c : classes) {
      if (c.getName().endsWith("CleanerThread")) {
        Field sleepTimeField = c.getDeclaredField("sleepTime");
        sleepTimeField.setAccessible(true);
        sleepTimeField.set(null, CLEANUP_SLEEP_TIME * 1000);
      }
    }
    conf
    .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    SessionState.start(new CliSessionState(conf));
    msClient = new HiveMetaStoreClient(conf);
    driver = DriverFactory.newDriver(new QueryState.Builder().withGenerateNewQueryId(true).nonIsolated().withHiveConf(conf).build(), null, null);
    md = MessageFactory.getInstance().getDeserializer();
  }

  @Before
  public void setup() throws Exception {
    long now = System.currentTimeMillis() / 1000;
    startTime = 0;
    if (now > Integer.MAX_VALUE) fail("Bummer, time has fallen over the edge");
    else startTime = (int) now;
    firstEventId = msClient.getCurrentNotificationEventId().getEventId();
    DummyRawStoreFailEvent.setEventSucceed(true);
  }

  @After
  public void tearDown() {
    MockMetaStoreEventListener.clearEvents();
  }


  @Test
  public void createDatabase() throws Exception {
    String dbName = "createdb";
    String dbName2 = "createdb2";
    String dbLocationUri = testTempDir;
    String dbDescription = "no description";
    Database db = new Database(dbName, dbDescription, dbLocationUri, emptyParameters);
    msClient.createDatabase(db);

    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(1, rsp.getEventsSize());

    NotificationEvent event = rsp.getEvents().get(0);
    assertEquals(firstEventId + 1, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(HCatConstants.HCAT_CREATE_DATABASE_EVENT, event.getEventType());
    assertEquals(dbName, event.getDbName());
    assertNull(event.getTableName());

    // Parse the message field
    CreateDatabaseMessage createDbMsg = md.getCreateDatabaseMessage(event.getMessage());
    assertEquals(dbName, createDbMsg.getDB());


    // Verify the eventID was passed to the non-transactional listener
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_DATABASE, firstEventId + 1);

    // When hive.metastore.transactional.event.listeners is set,
    // a failed event should not create a new notification
    DummyRawStoreFailEvent.setEventSucceed(false);
    db = new Database(dbName2, dbDescription, dbLocationUri, emptyParameters);
    try {
      msClient.createDatabase(db);
      fail("Error: create database should've failed");
    } catch (Exception ex) {
      // expected
    }

    rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(1, rsp.getEventsSize());
  }

  @Test
  public void dropDatabase() throws Exception {
    String dbName = "dropdb";
    String dbName2 = "dropdb2";
    String dbLocationUri = testTempDir;
    String dbDescription = "no description";
    msClient.createDatabase(new Database(dbName, dbDescription, dbLocationUri, emptyParameters));

    // Get the DB for comparison below since it may include additional parameters
    Database db = msClient.getDatabase(dbName);
    // Drop the database
    msClient.dropDatabase(dbName);

    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(2, rsp.getEventsSize());

    NotificationEvent event = rsp.getEvents().get(1);
    assertEquals(firstEventId + 2, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(HCatConstants.HCAT_DROP_DATABASE_EVENT, event.getEventType());
    assertEquals(dbName, event.getDbName());
    assertNull(event.getTableName());

    // Parse the message field
    DropDatabaseMessage dropDbMsg = md.getDropDatabaseMessage(event.getMessage());
    assertEquals(dbName, dropDbMsg.getDB());
    assertEquals(db, dropDbMsg.getDatabaseObject());

    // Verify the eventID was passed to the non-transactional listener
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.DROP_DATABASE, firstEventId + 2);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_DATABASE, firstEventId + 1);

    // When hive.metastore.transactional.event.listeners is set,
    // a failed event should not create a new notification
    msClient.createDatabase(new Database(dbName2, dbDescription, dbLocationUri, emptyParameters));
    DummyRawStoreFailEvent.setEventSucceed(false);
    try {
      msClient.dropDatabase(dbName2);
      fail("Error: drop database should've failed");
    } catch (Exception ex) {
      // expected
    }

    rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(3, rsp.getEventsSize());
  }

  @Test
  public void createTable() throws Exception {
    String defaultDbName = "default";
    String tblName = "createtable";
    String tblName2 = "createtable2";
    String tblOwner = "me";
    String serdeLocation = testTempDir;
    FieldSchema col1 = new FieldSchema("col1", "int", "no comment");
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(col1);
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd =
        new StorageDescriptor(cols, serdeLocation, "input", "output", false, 0, serde, null, null,
            emptyParameters);
    Table table =
        new Table(tblName, defaultDbName, tblOwner, startTime, startTime, 0, sd, null,
            emptyParameters, null, null, null);
    msClient.createTable(table);

    // Get notifications from metastore
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(1, rsp.getEventsSize());
    NotificationEvent event = rsp.getEvents().get(0);
    assertEquals(firstEventId + 1, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(EventType.CREATE_TABLE.toString(), event.getEventType());
    assertEquals(defaultDbName, event.getDbName());
    assertEquals(tblName, event.getTableName());

    // Parse the message field
    CreateTableMessage createTblMsg = md.getCreateTableMessage(event.getMessage());
    assertEquals(defaultDbName, createTblMsg.getDB());
    assertEquals(tblName, createTblMsg.getTable());
    assertEquals(table, createTblMsg.getTableObj());

    // Verify the eventID was passed to the non-transactional listener
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_TABLE, firstEventId + 1);

    // When hive.metastore.transactional.event.listeners is set,
    // a failed event should not create a new notification
    table =
        new Table(tblName2, defaultDbName, tblOwner, startTime, startTime, 0, sd, null,
            emptyParameters, null, null, null);
    DummyRawStoreFailEvent.setEventSucceed(false);
    try {
      msClient.createTable(table);
      fail("Error: create table should've failed");
    } catch (Exception ex) {
      // expected
    }

    rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(1, rsp.getEventsSize());
  }

  @Test
  public void alterDatabase() throws Exception {
    String dbName = "alterdatabase";

    Database dbBefore =
        new Database(dbName, "", Paths.get(testTempDir, "alterdatabase").toString(), null);
    dbBefore.setOwnerName("me");

    // Event 1
    msClient.createDatabase(dbBefore);
    // get the db for comparison below since it may include additional parameters
    dbBefore = msClient.getDatabase(dbName);
    Database dbAfter = dbBefore.deepCopy();
    // Event 2
    dbAfter.setOwnerName("you");
    dbAfter.setLocationUri(Paths.get(testTempDir, "alterdatabase_copy").toString());
    msClient.alterDatabase(dbName, dbAfter);
    dbAfter = msClient.getDatabase(dbName);

    // Get notifications from metastore
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(2, rsp.getEventsSize());
    NotificationEvent event = rsp.getEvents().get(1);
    assertEquals(firstEventId + 2, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(HCatConstants.HCAT_ALTER_DATABASE_EVENT, event.getEventType());
    assertEquals(dbName, event.getDbName());

    // Parse the message field
    AlterDatabaseMessage alterDatabaseMessage = md.getAlterDatabaseMessage(event.getMessage());
    assertEquals(dbName, alterDatabaseMessage.getDB());
    assertEquals(dbBefore, alterDatabaseMessage.getDbObjBefore());
    assertEquals(dbAfter, alterDatabaseMessage.getDbObjAfter());

    // Verify the eventID was passed to the non-transactional listener
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.ALTER_DATABASE, firstEventId + 2);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_DATABASE, firstEventId + 1);

    // When hive.metastore.transactional.event.listeners is set,
    // a failed event should not create a new notification
    DummyRawStoreFailEvent.setEventSucceed(false);
    try {
      msClient.alterDatabase(dbName, dbAfter);
      fail("Error: alter database should've failed");
    } catch (Exception ex) {
      // expected
    }
    rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(2, rsp.getEventsSize());
  }

  @Test
  public void alterTable() throws Exception {
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, testTempDir, "input", "output", false, 0,
        serde, null, null, emptyParameters);
    Table table = new Table("alttable", "default", "me", startTime, startTime, 0, sd,
        new ArrayList<FieldSchema>(), emptyParameters, null, null, null);
    // Event 1
    msClient.createTable(table);
    // Need to modify table location as well
    sd.setLocation(Paths.get(testTempDir, "0").toString());
    cols.add(new FieldSchema("col2", "int", ""));
    table = new Table("alttable", "default", "me", startTime, startTime, 0, sd,
        new ArrayList<FieldSchema>(), emptyParameters, null, null, null);
    // Event 2
    msClient.alter_table("default", "alttable", table);

    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(2, rsp.getEventsSize());

    NotificationEvent event = rsp.getEvents().get(1);
    assertEquals(firstEventId + 2, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(HCatConstants.HCAT_ALTER_TABLE_EVENT, event.getEventType());
    assertEquals("default", event.getDbName());
    assertEquals("alttable", event.getTableName());

    AlterTableMessage alterTableMessage = md.getAlterTableMessage(event.getMessage());
    assertEquals(table, alterTableMessage.getTableObjAfter());

    // Verify the eventID was passed to the non-transactional listener
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.ALTER_TABLE, firstEventId + 2);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_TABLE, firstEventId + 1);

    // When hive.metastore.transactional.event.listeners is set,
    // a failed event should not create a new notification
    DummyRawStoreFailEvent.setEventSucceed(false);
    try {
      msClient.alter_table("default", "alttable", table);
    } catch (Exception ex) {
      // expected
    }

    rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(2, rsp.getEventsSize());
  }

  @Test
  public void dropTable() throws Exception {
    String defaultDbName = "default";
    String tblName = "droptbl";
    String tblName2 = "droptbl2";
    String tblOwner = "me";
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, testTempDir, "input", "output", false, 0,
        serde, null, null, emptyParameters);
    Table table = new Table(tblName, defaultDbName, tblOwner, startTime, startTime, 0, sd, null,
        emptyParameters, null, null, null);
    msClient.createTable(table);
    msClient.dropTable(defaultDbName, tblName);

    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(2, rsp.getEventsSize());

    NotificationEvent event = rsp.getEvents().get(1);
    assertEquals(firstEventId + 2, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(HCatConstants.HCAT_DROP_TABLE_EVENT, event.getEventType());
    assertEquals(defaultDbName, event.getDbName());
    assertEquals(tblName, event.getTableName());

    // Parse the message field
    DropTableMessage dropTblMsg = md.getDropTableMessage(event.getMessage());
    assertEquals(defaultDbName, dropTblMsg.getDB());
    assertEquals(tblName, dropTblMsg.getTable());
    Table tableObj = dropTblMsg.getTableObj();
    assertEquals(table.getDbName(), tableObj.getDbName());
    assertEquals(table.getTableName(), tableObj.getTableName());
    assertEquals(table.getOwner(), tableObj.getOwner());
    assertEquals(table.getParameters(), tableObj.getParameters());


    // Verify the eventID was passed to the non-transactional listener
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.DROP_TABLE, firstEventId + 2);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_TABLE, firstEventId + 1);

    // When hive.metastore.transactional.event.listeners is set,
    // a failed event should not create a new notification
    table = new Table(tblName2, defaultDbName, tblOwner, startTime, startTime, 0, sd, null,
                         emptyParameters, null, null, null);
    msClient.createTable(table);
    DummyRawStoreFailEvent.setEventSucceed(false);
    try {
      msClient.dropTable(defaultDbName, tblName2);
    } catch (Exception ex) {
      // expected
    }

    rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(3, rsp.getEventsSize());
  }

  @Test
  public void addPartition() throws Exception {
    String defaultDbName = "default";
    String tblName = "addptn";
    String tblName2 = "addptn2";
    String tblOwner = "me";
    String serdeLocation = testTempDir;
    FieldSchema col1 = new FieldSchema("col1", "int", "no comment");
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(col1);
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd =
        new StorageDescriptor(cols, serdeLocation, "input", "output", false, 0, serde, null, null,
            emptyParameters);
    FieldSchema partCol1 = new FieldSchema("ds", "string", "no comment");
    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> partCol1Vals = Arrays.asList("today");
    partCols.add(partCol1);
    Table table =
        new Table(tblName, defaultDbName, tblOwner, startTime, startTime, 0, sd, partCols,
            emptyParameters, null, null, null);

    // Event 1
    msClient.createTable(table);
    Partition partition =
        new Partition(partCol1Vals, defaultDbName, tblName, startTime, startTime, sd,
            emptyParameters);
    // Event 2
    msClient.add_partition(partition);

    // Get notifications from metastore
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(2, rsp.getEventsSize());
    NotificationEvent event = rsp.getEvents().get(1);
    assertEquals(firstEventId + 2, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(EventType.ADD_PARTITION.toString(), event.getEventType());
    assertEquals(defaultDbName, event.getDbName());
    assertEquals(tblName, event.getTableName());

    // Parse the message field
    AddPartitionMessage addPtnMsg = md.getAddPartitionMessage(event.getMessage());
    assertEquals(defaultDbName, addPtnMsg.getDB());
    assertEquals(tblName, addPtnMsg.getTable());
    Iterator<Partition> ptnIter = addPtnMsg.getPartitionObjs().iterator();
    assertTrue(ptnIter.hasNext());
    assertEquals(partition, ptnIter.next());

    // Verify the eventID was passed to the non-transactional listener
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.ADD_PARTITION, firstEventId + 2);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_TABLE, firstEventId + 1);

    // When hive.metastore.transactional.event.listeners is set,
    // a failed event should not create a new notification
    partition =
        new Partition(Arrays.asList("tomorrow"), defaultDbName, tblName2, startTime, startTime, sd,
            emptyParameters);
    DummyRawStoreFailEvent.setEventSucceed(false);
    try {
      msClient.add_partition(partition);
      fail("Error: add partition should've failed");
    } catch (Exception ex) {
      // expected
    }

    rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(2, rsp.getEventsSize());
  }

  @Test
  public void alterPartition() throws Exception {
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", "nocomment"));
    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    partCols.add(new FieldSchema("ds", "string", ""));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, testTempDir, "input", "output", false, 0,
        serde, null, null, emptyParameters);
    Table table = new Table("alterparttable", "default", "me", startTime, startTime, 0, sd,
        partCols, emptyParameters, null, null, null);
    msClient.createTable(table);

    Partition partition = new Partition(Arrays.asList("today"), "default", "alterparttable",
        startTime, startTime, sd, emptyParameters);
    msClient.add_partition(partition);
    // Need to modify table location as well
    sd.setLocation(Paths.get(testTempDir, "0").toString());


    Partition newPart = new Partition(Arrays.asList("today"), "default", "alterparttable",
        startTime, startTime + 1, sd, emptyParameters);
    msClient.alter_partition("default", "alterparttable", newPart, null);

    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(3, rsp.getEventsSize());
    NotificationEvent event = rsp.getEvents().get(2);
    assertEquals(firstEventId + 3, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(HCatConstants.HCAT_ALTER_PARTITION_EVENT, event.getEventType());
    assertEquals("default", event.getDbName());
    assertEquals("alterparttable", event.getTableName());

    // Parse the message field
    ObjectNode jsonTree = JSONMessageFactory.getJsonTree(event);
    assertEquals("default", jsonTree.get("db").asText());
    assertEquals("alterparttable", jsonTree.get("table").asText());
    AlterPartitionMessage alterPartitionMessage = md.getAlterPartitionMessage(event.getMessage());
    assertEquals(newPart, alterPartitionMessage.getPtnObjAfter());

    // Verify the eventID was passed to the non-transactional listener
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.ADD_PARTITION, firstEventId + 2);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_TABLE, firstEventId + 1);

    // When hive.metastore.transactional.event.listeners is set,
    // a failed event should not create a new notification
    DummyRawStoreFailEvent.setEventSucceed(false);
    try {
      msClient.alter_partition("default", "alterparttable", newPart, null);
    } catch (Exception ex) {
      // expected
    }

    rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(3, rsp.getEventsSize());
  }

  @Test
  public void dropPartition() throws Exception {
    String dbName = "default";
    String tblName = "dropparttable";
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", "nocomment"));
    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    partCols.add(new FieldSchema("ds", "string", ""));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, testTempDir, "input", "output", false, 0,
        serde, null, null, emptyParameters);
    Table table = new Table("dropPartTable", dbName, "me", startTime, startTime, 0, sd, partCols,
        emptyParameters, null, null, null);
    msClient.createTable(table);

    Partition partition = new Partition(Arrays.asList("today"), "default", "dropPartTable",
        startTime, startTime, sd, emptyParameters);
    msClient.add_partition(partition);

    msClient.dropPartition(dbName, "dropparttable", Arrays.asList("today"), false);

    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(3, rsp.getEventsSize());

    NotificationEvent event = rsp.getEvents().get(2);
    assertEquals(firstEventId + 3, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(HCatConstants.HCAT_DROP_PARTITION_EVENT, event.getEventType());
    assertEquals("default", event.getDbName());
    assertEquals(tblName, event.getTableName());

    // Parse the message field
    DropPartitionMessage dropPtnMsg = md.getDropPartitionMessage(event.getMessage());
    assertEquals(dbName, dropPtnMsg.getDB());
    assertEquals(tblName, dropPtnMsg.getTable());
    Table tableObj = dropPtnMsg.getTableObj();
    assertEquals(table.getDbName(), tableObj.getDbName());
    assertEquals(tblName, tableObj.getTableName());
    assertEquals(table.getOwner(), tableObj.getOwner());
    Iterator<Map<String, String>> parts = dropPtnMsg.getPartitions().iterator();
    assertTrue(parts.hasNext());
    assertEquals(partition.getValues(), Lists.newArrayList(parts.next().values()));

    // Verify the eventID was passed to the non-transactional listener
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.DROP_PARTITION, firstEventId + 3);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.ADD_PARTITION, firstEventId + 2);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_TABLE, firstEventId + 1);

    // When hive.metastore.transactional.event.listeners is set,
    // a failed event should not create a new notification
    partition = new Partition(Arrays.asList("tomorrow"), "default", "dropPartTable",
                                 startTime, startTime, sd, emptyParameters);
    msClient.add_partition(partition);
    DummyRawStoreFailEvent.setEventSucceed(false);
    try {
      msClient.dropPartition("default", "dropparttable", Arrays.asList("tomorrow"), false);
    } catch (Exception ex) {
      // expected
    }

    rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(4, rsp.getEventsSize());
  }

  @Test
  public void exchangePartition() throws Exception {
    String dbName = "default";
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", "nocomment"));
    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    partCols.add(new FieldSchema("part", "int", ""));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd1 =
        new StorageDescriptor(cols, Paths.get(testTempDir, "1").toString(), "input", "output", false, 0, serde, null,
            null, emptyParameters);
    Table tab1 = new Table("tab1", dbName, "me", startTime, startTime, 0, sd1, partCols,
        emptyParameters, null, null, null);
    msClient.createTable(tab1);
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(1, rsp.getEventsSize()); // add_table

    StorageDescriptor sd2 =
        new StorageDescriptor(cols, Paths.get(testTempDir, "2").toString(), "input", "output", false, 0, serde, null,
            null, emptyParameters);
    Table tab2 = new Table("tab2", dbName, "me", startTime, startTime, 0, sd2, partCols,
        emptyParameters, null, null, null); // add_table
    msClient.createTable(tab2);
    rsp = msClient.getNextNotification(firstEventId + 1, 0, null);
    assertEquals(1, rsp.getEventsSize());

    StorageDescriptor sd1part =
        new StorageDescriptor(cols, Paths.get(testTempDir, "1", "part=1").toString(), "input", "output", false, 0,
            serde, null, null, emptyParameters);
    StorageDescriptor sd2part =
        new StorageDescriptor(cols, Paths.get(testTempDir, "1", "part=2").toString(), "input", "output", false, 0,
            serde, null, null, emptyParameters);
    StorageDescriptor sd3part =
        new StorageDescriptor(cols, Paths.get(testTempDir, "1", "part=3").toString(), "input", "output", false, 0,
            serde, null, null, emptyParameters);
    Partition part1 = new Partition(Arrays.asList("1"), "default", tab1.getTableName(),
        startTime, startTime, sd1part, emptyParameters);
    Partition part2 = new Partition(Arrays.asList("2"), "default", tab1.getTableName(),
        startTime, startTime, sd2part, emptyParameters);
    Partition part3 = new Partition(Arrays.asList("3"), "default", tab1.getTableName(),
        startTime, startTime, sd3part, emptyParameters);
    msClient.add_partitions(Arrays.asList(part1, part2, part3));
    rsp = msClient.getNextNotification(firstEventId + 2, 0, null);
    assertEquals(1, rsp.getEventsSize()); // add_partition

    msClient.exchange_partition(ImmutableMap.of("part", "1"),
        dbName, tab1.getTableName(), dbName, tab2.getTableName());

    rsp = msClient.getNextNotification(firstEventId + 3, 0, null);
    assertEquals(2, rsp.getEventsSize());

    NotificationEvent event = rsp.getEvents().get(0);
    assertEquals(firstEventId + 4, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(HCatConstants.HCAT_ADD_PARTITION_EVENT, event.getEventType());
    assertEquals(dbName, event.getDbName());
    assertEquals(tab2.getTableName(), event.getTableName());

    // Parse the message field
    AddPartitionMessage addPtnMsg = md.getAddPartitionMessage(event.getMessage());
    assertEquals(dbName, addPtnMsg.getDB());
    assertEquals(tab2.getTableName(), addPtnMsg.getTable());
    Iterator<Partition> ptnIter = addPtnMsg.getPartitionObjs().iterator();

    event = rsp.getEvents().get(1);
    assertEquals(firstEventId + 5, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(HCatConstants.HCAT_DROP_PARTITION_EVENT, event.getEventType());
    assertEquals(dbName, event.getDbName());
    assertEquals(tab1.getTableName(), event.getTableName());

    // Parse the message field
    DropPartitionMessage dropPtnMsg = md.getDropPartitionMessage(event.getMessage());
    assertEquals(dbName, dropPtnMsg.getDB());
    assertEquals(tab1.getTableName(), dropPtnMsg.getTable());
    Table tableObj = dropPtnMsg.getTableObj();
    assertEquals(tab1.getDbName(), tableObj.getDbName());
    assertEquals(tab1.getTableName(), tableObj.getTableName());
    assertEquals(tab1.getOwner(), tableObj.getOwner());
    Iterator<Map<String, String>> parts = dropPtnMsg.getPartitions().iterator();
    assertTrue(parts.hasNext());
    assertEquals(part1.getValues(), Lists.newArrayList(parts.next().values()));

    // Verify the eventID was passed to the non-transactional listener
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.DROP_PARTITION, firstEventId + 5);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.ADD_PARTITION, firstEventId + 4);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.ADD_PARTITION, firstEventId + 3);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_TABLE, firstEventId + 2);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_TABLE, firstEventId + 1);
  }

  @Ignore("CREATE FUNCTION events are currently not generated")
  @Test
  public void createFunction() throws Exception {
    String funcName = "createFunction";
    String dbName = "default";
    String ownerName = "me";
    String funcClass = "o.a.h.h.myfunc";
    String funcResource = Paths.get(testTempDir, "somewhere").toString();
    Function func = new Function(funcName, dbName, funcClass, ownerName, PrincipalType.USER,
        startTime, FunctionType.JAVA, Arrays.asList(new ResourceUri(ResourceType.JAR,
        funcResource)));
    msClient.createFunction(func);
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(1, rsp.getEventsSize());
    NotificationEvent event = rsp.getEvents().get(0);
    assertEquals(firstEventId + 1, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(HCatConstants.HCAT_CREATE_FUNCTION_EVENT, event.getEventType());
    assertEquals(dbName, event.getDbName());
    Function funcObj = JSONMessageFactory.getFunctionObj(JSONMessageFactory.getJsonTree(event));
    assertEquals(dbName, funcObj.getDbName());
    assertEquals(funcName, funcObj.getFunctionName());
    assertEquals(funcClass, funcObj.getClassName());
    assertEquals(ownerName, funcObj.getOwnerName());
    assertEquals(FunctionType.JAVA, funcObj.getFunctionType());
    assertEquals(1, funcObj.getResourceUrisSize());
    assertEquals(ResourceType.JAR, funcObj.getResourceUris().get(0).getResourceType());
    assertEquals(funcResource, funcObj.getResourceUris().get(0).getUri());

    // Verify the eventID was passed to the non-transactional listener
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_FUNCTION, firstEventId + 1);

    // When hive.metastore.transactional.event.listeners is set,
    // a failed event should not create a new notification
    DummyRawStoreFailEvent.setEventSucceed(false);
    func = new Function("createFunction2", dbName, "o.a.h.h.myfunc2", "me", PrincipalType.USER,
        startTime, FunctionType.JAVA, Arrays.asList(new ResourceUri(ResourceType.JAR,
            Paths.get(testTempDir, "somewhere2").toString())));
    try {
      msClient.createFunction(func);
    } catch (Exception ex) {
      // expected
    }

    rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(1, rsp.getEventsSize());
  }

  @Ignore("DROP FUNCTION events are currently not generated")
  @Test
  public void dropFunction() throws Exception {
    String funcName = "dropfunctiontest";
    String dbName = "default";
    String ownerName = "me";
    String funcClass = "o.a.h.h.dropFunctionTest";
    String funcResource = Paths.get(testTempDir, "somewhere").toString();
    Function func = new Function(funcName, dbName, funcClass, ownerName, PrincipalType.USER,
        startTime, FunctionType.JAVA, Arrays.asList(new ResourceUri(ResourceType.JAR,
        funcResource)));
    msClient.createFunction(func);
    msClient.dropFunction(dbName, funcName);
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(2, rsp.getEventsSize());
    NotificationEvent event = rsp.getEvents().get(1);
    assertEquals(firstEventId + 2, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(HCatConstants.HCAT_DROP_FUNCTION_EVENT, event.getEventType());
    assertEquals(dbName, event.getDbName());
    Function funcObj = JSONMessageFactory.getFunctionObj(JSONMessageFactory.getJsonTree(event));
    assertEquals(dbName, funcObj.getDbName());
    assertEquals(funcName, funcObj.getFunctionName());
    assertEquals(funcClass, funcObj.getClassName());
    assertEquals(ownerName, funcObj.getOwnerName());
    assertEquals(FunctionType.JAVA, funcObj.getFunctionType());
    assertEquals(1, funcObj.getResourceUrisSize());
    assertEquals(ResourceType.JAR, funcObj.getResourceUris().get(0).getResourceType());
    assertEquals(funcResource, funcObj.getResourceUris().get(0).getUri());

    // Verify the eventID was passed to the non-transactional listener
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.DROP_FUNCTION, firstEventId + 2);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_FUNCTION, firstEventId + 1);

    // When hive.metastore.transactional.event.listeners is set,
    // a failed event should not create a new notification
    func = new Function("dropfunctiontest2", dbName, "o.a.h.h.dropFunctionTest2", "me",
                           PrincipalType.USER,  startTime, FunctionType.JAVA, Arrays.asList(
        new ResourceUri(ResourceType.JAR, Paths.get(testTempDir, "somewhere2").toString())));
    msClient.createFunction(func);
    DummyRawStoreFailEvent.setEventSucceed(false);
    try {
      msClient.dropFunction(dbName, "dropfunctiontest2");
    } catch (Exception ex) {
      // expected
    }

    rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(3, rsp.getEventsSize());
  }

  @Ignore("CREATE INDEX events are currently not generated")
  @Test
  public void createIndex() throws Exception {
    String indexName = "createIndex";
    String dbName = "default";
    String tableName = "createIndexTable";
    String indexTableName = tableName + "__" + indexName + "__";
    int startTime = (int)(System.currentTimeMillis() / 1000);
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", ""));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    Map<String, String> params = new HashMap<String, String>();
    params.put("key", "value");
    StorageDescriptor sd = new StorageDescriptor(cols, testTempDir, "input", "output", false, 17,
        serde, Arrays.asList("bucketcol"), Arrays.asList(new Order("sortcol", 1)), params);
    Table table = new Table(tableName, dbName, "me", startTime, startTime, 0, sd, null,
        emptyParameters, null, null, null);
    msClient.createTable(table);
    Index index = new Index(indexName, null, "default", tableName, startTime, startTime,
        indexTableName, sd, emptyParameters, false);
    Table indexTable = new Table(indexTableName, dbName, "me", startTime, startTime, 0, sd, null,
        emptyParameters, null, null, null);
    msClient.createIndex(index, indexTable);
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(3, rsp.getEventsSize());
    NotificationEvent event = rsp.getEvents().get(2);
    assertEquals(firstEventId + 3, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(HCatConstants.HCAT_CREATE_INDEX_EVENT, event.getEventType());
    assertEquals(dbName, event.getDbName());
    assertEquals(tableName.toLowerCase(), event.getTableName().toLowerCase());
    Index indexObj = JSONMessageFactory.getIndexObj(JSONMessageFactory.getJsonTree(event));
    assertEquals(dbName, indexObj.getDbName());
    assertEquals(indexName, indexObj.getIndexName());
    assertEquals(tableName, indexObj.getOrigTableName());
    assertEquals(indexTableName, indexObj.getIndexTableName());

    // Verify the eventID was passed to the non-transactional listener
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_INDEX, firstEventId + 3);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_TABLE, firstEventId + 2);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_TABLE, firstEventId + 1);

    // When hive.metastore.transactional.event.listeners is set,
    // a failed event should not create a new notification
    DummyRawStoreFailEvent.setEventSucceed(false);
    index = new Index("createIndexTable2", null, "default", tableName, startTime, startTime,
        "createIndexTable2__createIndexTable2__", sd, emptyParameters, false);
    Table indexTable2 = new Table("createIndexTable2__createIndexTable2__", dbName, "me",
        startTime, startTime, 0, sd, null, emptyParameters, null, null, null);
    try {
      msClient.createIndex(index, indexTable2);
    } catch (Exception ex) {
      // expected
    }

    rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(3, rsp.getEventsSize());
  }

  @Ignore("DROP INDEX events are currently not generated")
  @Test
  public void dropIndex() throws Exception {
    String indexName = "dropIndex";
    String dbName = "default";
    String tableName = "dropIndexTable";
    String indexTableName = tableName + "__" + indexName + "__";
    int startTime = (int)(System.currentTimeMillis() / 1000);
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", ""));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    Map<String, String> params = new HashMap<String, String>();
    params.put("key", "value");
    StorageDescriptor sd = new StorageDescriptor(cols, testTempDir, "input", "output", false, 17,
        serde, Arrays.asList("bucketcol"), Arrays.asList(new Order("sortcol", 1)), params);
    Table table = new Table(tableName, dbName, "me", startTime, startTime, 0, sd, null,
        emptyParameters, null, null, null);
    msClient.createTable(table);
    Index index = new Index(indexName, null, "default", tableName, startTime, startTime,
        indexTableName, sd, emptyParameters, false);
    Table indexTable = new Table(indexTableName, dbName, "me", startTime, startTime, 0, sd, null,
        emptyParameters, null, null, null);
    msClient.createIndex(index, indexTable);
    msClient.dropIndex(dbName, tableName, indexName, true); // drops index and indexTable
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(4, rsp.getEventsSize());
    NotificationEvent event = rsp.getEvents().get(3);
    assertEquals(firstEventId + 4, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(HCatConstants.HCAT_DROP_INDEX_EVENT, event.getEventType());
    assertEquals(dbName, event.getDbName());
    assertEquals(tableName.toLowerCase(), event.getTableName().toLowerCase());
    Index indexObj = JSONMessageFactory.getIndexObj(JSONMessageFactory.getJsonTree(event));
    assertEquals(dbName, indexObj.getDbName());
    assertEquals(indexName.toLowerCase(), indexObj.getIndexName());
    assertEquals(tableName.toLowerCase(), indexObj.getOrigTableName());
    assertEquals(indexTableName.toLowerCase(), indexObj.getIndexTableName());

    // Verify the eventID was passed to the non-transactional listener
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.DROP_INDEX, firstEventId + 4);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_INDEX, firstEventId + 3);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_TABLE, firstEventId + 2);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_TABLE, firstEventId + 1);

    // When hive.metastore.transactional.event.listeners is set,
    // a failed event should not create a new notification
    index = new Index("dropIndexTable2", null, "default", tableName, startTime, startTime,
                         "dropIndexTable__dropIndexTable2__", sd, emptyParameters, false);
    Table indexTable2 = new Table("dropIndexTable__dropIndexTable2__", dbName, "me", startTime,
                                     startTime, 0, sd, null, emptyParameters, null, null, null);
    msClient.createIndex(index, indexTable2);
    DummyRawStoreFailEvent.setEventSucceed(false);
    try {
      msClient.dropIndex(dbName, tableName, "dropIndex2", true); // drops index and indexTable
    } catch (Exception ex) {
      // expected
    }

    rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(6, rsp.getEventsSize());
  }

  @Ignore("ALTER INDEX events are currently not generated")
  @Test
  public void alterIndex() throws Exception {
    String indexName = "alterIndex";
    String dbName = "default";
    String tableName = "alterIndexTable";
    String indexTableName = tableName + "__" + indexName + "__";
    int startTime = (int)(System.currentTimeMillis() / 1000);
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", ""));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    Map<String, String> params = new HashMap<String, String>();
    params.put("key", "value");
    StorageDescriptor sd = new StorageDescriptor(cols, testTempDir, "input", "output", false, 17,
        serde, Arrays.asList("bucketcol"), Arrays.asList(new Order("sortcol", 1)), params);
    Table table = new Table(tableName, dbName, "me", startTime, startTime, 0, sd, null,
        emptyParameters, null, null, null);
    msClient.createTable(table);
    Index oldIndex = new Index(indexName, null, "default", tableName, startTime, startTime,
        indexTableName, sd, emptyParameters, false);
    Table oldIndexTable = new Table(indexTableName, dbName, "me", startTime, startTime, 0, sd, null,
        emptyParameters, null, null, null);
    msClient.createIndex(oldIndex, oldIndexTable); // creates index and index table
    Index newIndex = new Index(indexName, null, "default", tableName, startTime, startTime + 1,
        indexTableName, sd, emptyParameters, false);
    msClient.alter_index(dbName, tableName, indexName, newIndex);
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(4, rsp.getEventsSize());
    NotificationEvent event = rsp.getEvents().get(3);
    assertEquals(firstEventId + 4, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(HCatConstants.HCAT_ALTER_INDEX_EVENT, event.getEventType());
    assertEquals(dbName, event.getDbName());
    assertEquals(tableName.toLowerCase(), event.getTableName().toLowerCase());
    Index indexObj = JSONMessageFactory.getIndexObj(JSONMessageFactory.getJsonTree(event), "afterIndexObjJson");
    assertEquals(dbName, indexObj.getDbName());
    assertEquals(indexName, indexObj.getIndexName());
    assertEquals(tableName, indexObj.getOrigTableName());
    assertEquals(indexTableName, indexObj.getIndexTableName());
    assertTrue(indexObj.getCreateTime() < indexObj.getLastAccessTime());

    // Verify the eventID was passed to the non-transactional listener
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.ALTER_INDEX, firstEventId + 4);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_INDEX, firstEventId + 3);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_TABLE, firstEventId + 2);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_TABLE, firstEventId + 1);

    // When hive.metastore.transactional.event.listeners is set,
    // a failed event should not create a new notification
    DummyRawStoreFailEvent.setEventSucceed(false);
    try {
      msClient.alter_index(dbName, tableName, indexName, newIndex);
    } catch (Exception ex) {
      // expected
    }

    rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(4, rsp.getEventsSize());
  }

  @Test
  public void insertTable() throws Exception {
    String defaultDbName = "default";
    String tblName = "inserttbl";
    String serdeLocation = testTempDir;
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, serdeLocation, "input", "output", false, 0,
        serde, null, null, emptyParameters);
    Table table = new Table(tblName, defaultDbName, "me", startTime, startTime, 0, sd, null,
        emptyParameters, null, null, null);
    msClient.createTable(table);

    FireEventRequestData data = new FireEventRequestData();
    InsertEventRequestData insertData = new InsertEventRequestData();
    data.setInsertData(insertData);
    insertData.addToFilesAdded("/warehouse/mytable/b1");
    insertData.setReplace(false);
    FireEventRequest rqst = new FireEventRequest(true, data);
    rqst.setDbName(defaultDbName);
    rqst.setTableName(tblName);
    msClient.fireListenerEvent(rqst);

    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(2, rsp.getEventsSize());

    NotificationEvent event = rsp.getEvents().get(1);
    assertEquals(firstEventId + 2, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(HCatConstants.HCAT_INSERT_EVENT, event.getEventType());
    assertEquals(defaultDbName, event.getDbName());
    assertEquals(tblName, event.getTableName());

    // Parse the message field
    verifyInsert(event, defaultDbName, tblName);

    InsertMessage insertMsg = md.getInsertMessage(event.getMessage());
    assertEquals(defaultDbName, insertMsg.getDB());
    assertEquals(tblName, insertMsg.getTable());
    assertFalse(insertMsg.isReplace());

    // Verify the eventID was passed to the non-transactional listener
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.INSERT, firstEventId + 2);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_TABLE, firstEventId + 1);
  }

  @Test
  public void insertPartition() throws Exception {
    String defaultDbName = "default";
    String tblName = "insertptn";
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", "nocomment"));
    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    partCols.add(new FieldSchema("ds", "string", ""));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, testTempDir, "input", "output", false, 0,
        serde, null, null, emptyParameters);
    Table table = new Table(tblName, defaultDbName, "me", startTime, startTime, 0, sd,
        partCols, emptyParameters, null, null, null);
    msClient.createTable(table);
    List<String> partKeyVals = new ArrayList<String>();
    partKeyVals.add("today");
    Partition partition = new Partition(partKeyVals, defaultDbName, tblName,
        startTime, startTime, sd, emptyParameters);
    msClient.add_partition(partition);

    FireEventRequestData data = new FireEventRequestData();
    InsertEventRequestData insertData = new InsertEventRequestData();
    data.setInsertData(insertData);
    insertData.addToFilesAdded("/warehouse/mytable/today/b1");
    insertData.setReplace(false);
    FireEventRequest rqst = new FireEventRequest(true, data);
    rqst.setDbName(defaultDbName);
    rqst.setTableName(tblName);
    rqst.setPartitionVals(Arrays.asList("today"));
    msClient.fireListenerEvent(rqst);

    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(3, rsp.getEventsSize());

    NotificationEvent event = rsp.getEvents().get(2);
    assertEquals(firstEventId + 3, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(HCatConstants.HCAT_INSERT_EVENT, event.getEventType());
    assertEquals(defaultDbName, event.getDbName());
    assertEquals(tblName, event.getTableName());

    // Parse the message field
    verifyInsert(event, defaultDbName, tblName);
    InsertMessage insertMessage = md.getInsertMessage(event.getMessage());
    List<String> ptnValues = insertMessage.getPtnObj().getValues();
    assertFalse(insertMessage.isReplace());
    assertEquals(partKeyVals, ptnValues);

    // Verify the eventID was passed to the non-transactional listener
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.INSERT, firstEventId + 3);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.ADD_PARTITION, firstEventId + 2);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_TABLE, firstEventId + 1);
  }

  @Test
  public void getOnlyMaxEvents() throws Exception {
    Database db = new Database("db1", "no description", testTempDir, emptyParameters);
    msClient.createDatabase(db);
    db = new Database("db2", "no description", testTempDir, emptyParameters);
    msClient.createDatabase(db);
    db = new Database("db3", "no description", testTempDir, emptyParameters);
    msClient.createDatabase(db);

    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 2, null);
    assertEquals(2, rsp.getEventsSize());
    assertEquals(firstEventId + 1, rsp.getEvents().get(0).getEventId());
    assertEquals(firstEventId + 2, rsp.getEvents().get(1).getEventId());
  }

  @Test
  public void filter() throws Exception {
    Database db = new Database("f1", "no description", testTempDir, emptyParameters);
    msClient.createDatabase(db);
    db = new Database("f2", "no description", testTempDir, emptyParameters);
    msClient.createDatabase(db);
    msClient.dropDatabase("f2");

    IMetaStoreClient.NotificationFilter filter = new IMetaStoreClient.NotificationFilter() {
      @Override
      public boolean accept(NotificationEvent event) {
        return event.getEventType().equals(HCatConstants.HCAT_DROP_DATABASE_EVENT);
      }
    };

    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, filter);
    assertEquals(1, rsp.getEventsSize());
    assertEquals(firstEventId + 3, rsp.getEvents().get(0).getEventId());
  }

  @Test
  public void filterWithMax() throws Exception {
    Database db = new Database("f10", "no description", testTempDir, emptyParameters);
    msClient.createDatabase(db);
    db = new Database("f11", "no description", testTempDir, emptyParameters);
    msClient.createDatabase(db);
    msClient.dropDatabase("f11");

    IMetaStoreClient.NotificationFilter filter = new IMetaStoreClient.NotificationFilter() {
      @Override
      public boolean accept(NotificationEvent event) {
        return event.getEventType().equals(HCatConstants.HCAT_CREATE_DATABASE_EVENT);
      }
    };

    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 1, filter);
    assertEquals(1, rsp.getEventsSize());
    assertEquals(firstEventId + 1, rsp.getEvents().get(0).getEventId());
  }

  @Test
  public void sqlInsertTable() throws Exception {
    String defaultDbName = "default";
    String tblName = "sqlins";
    // Event 1
    driver.run("create table " + tblName + " (c int)");
    // Event 2 (insert)
    driver.run("insert into table " + tblName + " values (1)");
    // (No alter event generated as METASTORE_ALTER_NOTIFICATIONS_BASIC=true by default)
    driver.run("alter table " + tblName + " add columns (c2 int)");
    // Event 3
    driver.run("drop table " + tblName);

    // Get notifications from metastore
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(3, rsp.getEventsSize());
    NotificationEvent event = rsp.getEvents().get(0);
    assertEquals(firstEventId + 1, event.getEventId());
    assertEquals(EventType.CREATE_TABLE.toString(), event.getEventType());

    event = rsp.getEvents().get(1);
    assertEquals(firstEventId + 2, event.getEventId());
    assertEquals(EventType.INSERT.toString(), event.getEventType());
    // Parse the message field
    verifyInsert(event, defaultDbName, tblName);
    InsertMessage insertMsg = md.getInsertMessage(event.getMessage());
    assertFalse(insertMsg.isReplace());

    event = rsp.getEvents().get(2);
    assertEquals(firstEventId + 3, event.getEventId());
    assertEquals(EventType.DROP_TABLE.toString(), event.getEventType());
  }

  @Test
  public void sqlCTAS() throws Exception {
    // Force only basic stats notifications. Otherwise, additional ALTER notifications will be generated.
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_ALTER_NOTIFICATIONS_BASIC, true);

    String sourceTblName = "sqlctasins1";
    String targetTblName = "sqlctasins2";
    driver.run("create table " + sourceTblName + " (c int)");
    driver.run("insert into table " + sourceTblName + " values (1)");
    driver.run("create table " + targetTblName + " as select c from " + sourceTblName);

    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);

    assertEquals(3, rsp.getEventsSize());
    NotificationEvent event = rsp.getEvents().get(0);
    assertEquals(firstEventId + 1, event.getEventId());
    assertEquals(HCatConstants.HCAT_CREATE_TABLE_EVENT, event.getEventType());
    event = rsp.getEvents().get(1);
    assertEquals(firstEventId + 2, event.getEventId());
    assertEquals(HCatConstants.HCAT_INSERT_EVENT, event.getEventType());
    // Parse the message field
    verifyInsert(event, null, sourceTblName);

    event = rsp.getEvents().get(2);
    assertEquals(firstEventId + 3, event.getEventId());
    assertEquals(HCatConstants.HCAT_CREATE_TABLE_EVENT, event.getEventType());

    // Reset back to its original value.
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_ALTER_NOTIFICATIONS_BASIC, HiveConf.ConfVars.METASTORE_ALTER_NOTIFICATIONS_BASIC.defaultBoolVal);
  }

  @Test
  public void sqlTempTable() throws Exception {

    LOG.info("XXX Starting temp table");
    driver.run("create temporary table tmp1 (c int)");
    driver.run("insert into table tmp1 values (1)");

    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);

    assertEquals(0, rsp.getEventsSize());
  }

  @Test
  public void sqlDb() throws Exception {

    driver.run("create database sd");
    driver.run("drop database sd");

    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(2, rsp.getEventsSize());
    NotificationEvent event = rsp.getEvents().get(0);
    assertEquals(firstEventId + 1, event.getEventId());
    assertEquals(HCatConstants.HCAT_CREATE_DATABASE_EVENT, event.getEventType());
    event = rsp.getEvents().get(1);
    assertEquals(firstEventId + 2, event.getEventId());
    assertEquals(HCatConstants.HCAT_DROP_DATABASE_EVENT, event.getEventType());
  }

  @Test
  public void sqlInsertPartition() throws Exception {
    // Force only basic stats notifications. Otherwise, additional ALTER notifications will be generated.
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_ALTER_NOTIFICATIONS_BASIC, true);

    String tblName = "sqlinsptn";
    //create_table
    driver.run("create table " + tblName + " (c int) partitioned by (ds string)");
    //add_partition
    driver.run("insert into table " + tblName + " partition (ds = 'today') values (1)");
    //insert
    driver.run("insert into table " + tblName + " partition (ds = 'today') values (2)");
    //insert
    driver.run("insert into table " + tblName + " partition (ds) values (3, 'today')");
    //add_partition
    driver.run("alter table " + tblName + " add partition (ds = 'yesterday')");
    //insert
    driver.run("insert into table " + tblName + " partition (ds = 'yesterday') values (2)");
    //insert
    driver.run("insert into table " + tblName + " partition (ds) values (3, 'yesterday')");
    //add_partition
    driver.run("insert into table " + tblName + " partition (ds) values (3, 'tomorrow')");
    //drop_partition
    driver.run("alter table " + tblName + " drop partition (ds = 'tomorrow')");
    //add_partition
    driver.run("insert into table " + tblName + " partition (ds) values (42, 'todaytwo')");
    //insert
    driver.run("insert overwrite table " + tblName + " partition(ds='todaytwo') select c from "
        + tblName + " where 'ds'='today'");

    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);

    for (NotificationEvent ne : rsp.getEvents()) LOG.debug("EVENT: " + ne.getMessage());
    // For reasons not clear to me there's one or more alter partitions after add partition and
    // insert.
    assertEquals(11, rsp.getEventsSize());
    NotificationEvent event = rsp.getEvents().get(1);
    assertEquals(firstEventId + 2, event.getEventId());
    assertEquals(HCatConstants.HCAT_ADD_PARTITION_EVENT, event.getEventType());

    event = rsp.getEvents().get(2);
    assertEquals(firstEventId + 3, event.getEventId());
    assertEquals(HCatConstants.HCAT_INSERT_EVENT, event.getEventType());
    // Parse the message field
    verifyInsert(event, null, tblName);
    // Verify the replace flag.
    InsertMessage insertMsg = md.getInsertMessage(event.getMessage());
    assertFalse(insertMsg.isReplace());

    event = rsp.getEvents().get(3);
    assertEquals(firstEventId + 4, event.getEventId());
    assertEquals(HCatConstants.HCAT_INSERT_EVENT, event.getEventType());
    // Parse the message field
    verifyInsert(event, null, tblName);

    event = rsp.getEvents().get(4);
    assertEquals(firstEventId + 5, event.getEventId());
    assertEquals(HCatConstants.HCAT_ADD_PARTITION_EVENT, event.getEventType());

    event = rsp.getEvents().get(5);
    assertEquals(firstEventId + 6, event.getEventId());
    assertEquals(HCatConstants.HCAT_INSERT_EVENT, event.getEventType());
    // Parse the message field
    verifyInsert(event, null, tblName);

    event = rsp.getEvents().get(6);
    assertEquals(firstEventId + 7, event.getEventId());
    assertEquals(HCatConstants.HCAT_INSERT_EVENT, event.getEventType());
    // Parse the message field
    verifyInsert(event, null, tblName);

    event = rsp.getEvents().get(7);
    assertEquals(firstEventId + 8, event.getEventId());
    assertEquals(HCatConstants.HCAT_ADD_PARTITION_EVENT, event.getEventType());

    event = rsp.getEvents().get(8);
    assertEquals(firstEventId + 9, event.getEventId());
    assertEquals(HCatConstants.HCAT_DROP_PARTITION_EVENT, event.getEventType());

    event = rsp.getEvents().get(9);
    assertEquals(firstEventId + 10, event.getEventId());
    assertEquals(HCatConstants.HCAT_ADD_PARTITION_EVENT, event.getEventType());

    event = rsp.getEvents().get(10);
    assertEquals(firstEventId + 11, event.getEventId());
    assertEquals(HCatConstants.HCAT_INSERT_EVENT, event.getEventType());

    // Verify the replace flag.
    insertMsg = md.getInsertMessage(event.getMessage());
    assertTrue(insertMsg.isReplace());
    // replace-overwrite introduces no new files
    assertTrue(event.getMessage().matches(".*\"files\":\\[\\].*"));

    // Reset back to its original value.
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_ALTER_NOTIFICATIONS_BASIC, HiveConf.ConfVars.METASTORE_ALTER_NOTIFICATIONS_BASIC.defaultBoolVal);
   }

  private void verifyInsert(NotificationEvent event, String dbName, String tblName) throws Exception {
    // Parse the message field
    InsertMessage insertMsg = md.getInsertMessage(event.getMessage());

    if (dbName != null ){
      assertEquals(dbName, insertMsg.getTableObj().getDbName());
    }
    if (tblName != null){
      assertEquals(tblName, insertMsg.getTableObj().getTableName());
    }
    // Should have files
    Iterator<String> files = insertMsg.getFiles().iterator();
    assertTrue(files.hasNext());
    assertTrue(files.hasNext());
  }

  @Test
  public void cleanupNotifs() throws Exception {
    Database db = new Database("cleanup1", "no description", testTempDir, emptyParameters);
    msClient.createDatabase(db);
    msClient.dropDatabase("cleanup1");

    LOG.info("Pulling events immediately after createDatabase/dropDatabase");
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(2, rsp.getEventsSize());

    // sleep for expiry time, and then fetch again
    Thread.sleep(EVENTS_TTL * 2 * 1000); // sleep twice the TTL interval - things should have been cleaned by then.

    LOG.info("Pulling events again after cleanup");
    NotificationEventResponse rsp2 = msClient.getNextNotification(firstEventId, 0, null);
    LOG.info("second trigger done");
    assertEquals(0, rsp2.getEventsSize());
  }

  /**
   * This test is not available upstream as we have added CDH-specific flag
   * to enable all alter notifications or only the ones Sentry cares about.
   * Please refer JIRA CDH-72818 for information.
   * @throws Exception
   */
  @Test
  public void testAlterNotificationFlag() throws Exception {
    // When METASTORE_ALTER_NOTIFICATIONS_BASIC is false, we will see additional alter_events.
    // By default when it is true, alter notifications are created only
    // if tableName, dbName, location or owner info changes.
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_ALTER_NOTIFICATIONS_BASIC, false);
    try {
      driver.run("create table tbl_test (c int)");
      driver.run("insert into table tbl_test values (1),(2)");
      driver.run("drop table tbl_test");

      NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
      // Event 1 = CREATE_TABLE,Event 2 = ALTER_TABLE (Marker stats), Event 3 = INSERT,
      // Event 4 = ALTER_TABLE (Stats update), Event 5 = DROP_TABLE
      assertEquals(5, rsp.getEventsSize());
      NotificationEvent event = rsp.getEvents().get(0);
      assertEquals(firstEventId + 1, event.getEventId());
      assertEquals(HCatConstants.HCAT_CREATE_TABLE_EVENT, event.getEventType());
      event = rsp.getEvents().get(1);
      assertEquals(firstEventId + 2, event.getEventId());
      assertEquals(HCatConstants.HCAT_ALTER_TABLE_EVENT, event.getEventType());
      event = rsp.getEvents().get(2);
      assertEquals(firstEventId + 3, event.getEventId());
      assertEquals(HCatConstants.HCAT_INSERT_EVENT, event.getEventType());
      event = rsp.getEvents().get(3);
      assertEquals(firstEventId + 4, event.getEventId());
      assertEquals(HCatConstants.HCAT_ALTER_TABLE_EVENT, event.getEventType());
      event = rsp.getEvents().get(4);
      assertEquals(firstEventId + 5, event.getEventId());
      assertEquals(HCatConstants.HCAT_DROP_TABLE_EVENT, event.getEventType());
    } finally {
      // TearDown : Set config back to default value
      conf.setBoolVar(HiveConf.ConfVars.METASTORE_ALTER_NOTIFICATIONS_BASIC, true);
    }
  }

  /**
   * This test is not available upstream as we have added CDH-specific flag
   * to enable/disable stats related Alter event notifications.
   * Please refer JIRA CDH-72818 for information.
   * @throws Exception
   */
  @Test
  public void testDisableStatsNotificationsFlag() throws Exception {
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_ALTER_NOTIFICATIONS_BASIC, false);
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_DISABLE_STATS_NOTIFICATIONS, true);
    try {
      driver.run("create table tbl_flag_test (c int)");
      driver.run("insert into table tbl_flag_test values (1),(2)");
      driver.run("drop table tbl_flag_test");

      NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
      // Event 1 = CREATE_TABLE, Event 2 = INSERT, Event 3 = ALTER_TABLE (Marker stats),
      // (ALTER_TABLE for Stats update is disabled here) Event 4 = DROP_TABLE
      assertEquals(4, rsp.getEventsSize());
      NotificationEvent event = rsp.getEvents().get(0);
      assertEquals(firstEventId + 1, event.getEventId());
      assertEquals(HCatConstants.HCAT_CREATE_TABLE_EVENT, event.getEventType());
      event = rsp.getEvents().get(1);
      assertEquals(firstEventId + 2, event.getEventId());
      assertEquals(HCatConstants.HCAT_ALTER_TABLE_EVENT, event.getEventType());
      event = rsp.getEvents().get(2);
      assertEquals(firstEventId + 3, event.getEventId());
      assertEquals(HCatConstants.HCAT_INSERT_EVENT, event.getEventType());
      event = rsp.getEvents().get(3);
      assertEquals(firstEventId + 4, event.getEventId());
      assertEquals(HCatConstants.HCAT_DROP_TABLE_EVENT, event.getEventType());
    } finally {
      // TearDown : Set configs back to default values
      conf.setBoolVar(HiveConf.ConfVars.METASTORE_ALTER_NOTIFICATIONS_BASIC, true);
      conf.setBoolVar(HiveConf.ConfVars.METASTORE_DISABLE_STATS_NOTIFICATIONS, false);
    }
  }

}
