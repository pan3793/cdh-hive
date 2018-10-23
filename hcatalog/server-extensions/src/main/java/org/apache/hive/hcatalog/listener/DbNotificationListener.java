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
package org.apache.hive.hcatalog.listener;

import org.apache.commons.lang.StringUtils;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.MetaStoreEventListenerConstants;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.RawStoreProxy;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.events.AddIndexEvent;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.AlterIndexEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.ConfigChangeEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateFunctionEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropFunctionEvent;
import org.apache.hadoop.hive.metastore.events.DropIndexEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.InsertEvent;
import org.apache.hadoop.hive.metastore.events.LoadPartitionDoneEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage.EventType;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link org.apache.hadoop.hive.metastore.MetaStoreEventListener} that
 * stores events in the database.
 *
 * Design overview:  This listener takes any event, builds a NotificationEventResponse,
 * and puts it on a queue.  There is a dedicated thread that reads entries from the queue and
 * places them in the database.  The reason for doing it in a separate thread is that we want to
 * avoid slowing down other metadata operations with the work of putting the notification into
 * the database.  Also, occasionally the thread needs to clean the database of old records.  We
 * definitely don't want to do that as part of another metadata operation.
 *
 * NOTE: This listener is modified to skip events that are not used by Sentry. Currently
 * Sentry is the only consumer and some workloads generate huge  events that are not
 * useful for Sentry.
 */
public class DbNotificationListener extends MetaStoreEventListener {

  private static final Logger LOG = LoggerFactory.getLogger(DbNotificationListener.class.getName());
  private static CleanerThread cleaner = null;

  // This is the same object as super.conf, but it's convenient to keep a copy of it as a
  // HiveConf rather than a Configuration.
  private HiveConf hiveConf;
  private MessageFactory msgFactory;
  private final boolean alterStatNotificationDisabled;
  private final boolean alterNotificationsBasic;

  private synchronized void init(HiveConf conf) throws MetaException {
    if (cleaner == null) {
      cleaner =
          new CleanerThread(conf, RawStoreProxy.getProxy(conf, conf,
              conf.getVar(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL), 999999));
      cleaner.start();
    }
  }

  public DbNotificationListener(Configuration config) throws MetaException {
    super(config);
    // The code in MetastoreUtils.getMetaStoreListeners() that calls this looks for a constructor
    // with a Configuration parameter, so we have to declare config as Configuration.  But it
    // actually passes a HiveConf, which we need.  So we'll do this ugly down cast.
    hiveConf = (HiveConf)config;
    init(hiveConf);
    alterNotificationsBasic = HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.METASTORE_ALTER_NOTIFICATIONS_BASIC);
    alterStatNotificationDisabled =
        HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.METASTORE_DISABLE_STATS_NOTIFICATIONS);
    msgFactory = MessageFactory.getInstance();
  }

  /**
   * @param tableEvent table event.
   * @throws org.apache.hadoop.hive.metastore.api.MetaException
   */
  public void onConfigChange(ConfigChangeEvent tableEvent) throws MetaException {
    String key = tableEvent.getKey();
    if (key.equals(HiveConf.ConfVars.METASTORE_EVENT_DB_LISTENER_TTL.toString())) {
      // This weirdness of setting it in our hiveConf and then reading back does two things.
      // One, it handles the conversion of the TimeUnit.  Two, it keeps the value around for
      // later in case we need it again.
      hiveConf.set(HiveConf.ConfVars.METASTORE_EVENT_DB_LISTENER_TTL.name(),
          tableEvent.getNewValue());
      cleaner.setTimeToLive(hiveConf.getTimeVar(HiveConf.ConfVars.METASTORE_EVENT_DB_LISTENER_TTL,
          TimeUnit.SECONDS));
    }
  }

  /**
   * @param tableEvent table event.
   * @throws MetaException
   */
  public void onCreateTable(CreateTableEvent tableEvent) throws MetaException {
    Table t = tableEvent.getTable();
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.CREATE_TABLE.toString(), msgFactory
            .buildCreateTableMessage(t).toString());
    event.setDbName(t.getDbName());
    event.setTableName(t.getTableName());
    enqueue(event, tableEvent);
  }

  /**
   * @param tableEvent table event.
   * @throws MetaException
   */
  public void onDropTable(DropTableEvent tableEvent) throws MetaException {
    Table t = tableEvent.getTable();
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.DROP_TABLE.toString(), msgFactory
            .buildDropTableMessage(t).toString());
    event.setDbName(t.getDbName());
    event.setTableName(t.getTableName());
    enqueue(event, tableEvent);
  }

  /**
   * @param tableEvent alter table event
   * @throws MetaException
   */
  public void onAlterTable(AlterTableEvent tableEvent) throws MetaException {

    EnvironmentContext environmentContext = tableEvent.getEnvironmentContext();
    if (alterStatNotificationDisabled && environmentContext != null && environmentContext.isSetProperties()
        && environmentContext.getProperties().containsKey(StatsSetupConst.STATS_GENERATED)) {
      // This means alter event is called for stats generated by a task or user.
      // Hence, we disable publishing this notification if HiveConf.METASTORE_DISABLE_STATS_NOTIFICATIONS
      // is set to true.
      return;
    }

    Table before = tableEvent.getOldTable();
    Table after = tableEvent.getNewTable();

    if (alterNotificationsBasic) {
      // Verify whether either the name of the db, the name of the table, the location or the object owner changed.
      if (before.getDbName() == null || after.getDbName() == null || before.getTableName() == null
          || after.getTableName() == null) {
        return;
      }

      if (before.getSd() == null || after.getSd() == null) {
        return;
      }

      // Only check for null locations if it is not a view
      if (!isVirtualView(before) && before.getSd().getLocation() == null) {
        return;
      }

      // Only check for null locations if it is not a view
      if (!isVirtualView(after) && after.getSd().getLocation() == null) {
        return;
      }

      if (before.getDbName().equals(after.getDbName()) && before.getTableName().equals(after.getTableName())
          && StringUtils.equals(before.getSd().getLocation(), after.getSd().getLocation())
          && before.getOwnerType() == after.getOwnerType() && StringUtils.equals(before.getOwner(), after.getOwner())) {
        // Nothing interesting changed
        return;
      }
    }

    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.ALTER_TABLE.toString(), msgFactory
            .buildAlterTableMessage(before, after).toString());
    event.setDbName(after.getDbName());
    event.setTableName(after.getTableName());
    enqueue(event, tableEvent);
  }

  /**
   * @param partitionEvent partition event
   * @throws MetaException
   */
  public void onAddPartition(AddPartitionEvent partitionEvent) throws MetaException {
    Table t = partitionEvent.getTable();
    String msg = msgFactory
        .buildAddPartitionMessage(t, partitionEvent.getPartitionIterator()).toString();
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.ADD_PARTITION.toString(), msg);
    event.setDbName(t.getDbName());
    event.setTableName(t.getTableName());
    enqueue(event, partitionEvent);
  }

  /**
   * @param partitionEvent partition event
   * @throws MetaException
   */
  public void onDropPartition(DropPartitionEvent partitionEvent) throws MetaException {
    Table t = partitionEvent.getTable();
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.DROP_PARTITION.toString(), msgFactory
            .buildDropPartitionMessage(t, partitionEvent.getPartitionIterator()).toString());
    event.setDbName(t.getDbName());
    event.setTableName(t.getTableName());
    enqueue(event, partitionEvent);
  }

  /**
   * @param partitionEvent partition event
   * @throws MetaException
   */
  public void onAlterPartition(AlterPartitionEvent partitionEvent) throws MetaException {

    EnvironmentContext environmentContext = partitionEvent.getEnvironmentContext();
    if (alterStatNotificationDisabled && environmentContext != null && environmentContext.isSetProperties()
        && environmentContext.getProperties().containsKey(StatsSetupConst.STATS_GENERATED)) {
      // This means alter event is called for stats generated by a task or user.
      // Hence, we disable publishing this notification if HiveConf.METASTORE_DISABLE_STATS_NOTIFICATIONS
      // is set to true.
      return;
    }

    Partition before = partitionEvent.getOldPartition();
    Partition after = partitionEvent.getNewPartition();

    if (alterNotificationsBasic) {
      // Verify whether either the name of the db or table changed or location changed.
      if (before.getSd() == null || after.getSd() == null) {
        return;
      }
      if (before.getSd().getLocation() == null || after.getSd().getLocation() == null) {
        return;
      }

      if (before.getDbName().equals(after.getDbName()) && before.getTableName().equals(after.getTableName()) && before
          .getSd().getLocation().equals(after.getSd().getLocation())) {
        return;
      }
    }

    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.ALTER_PARTITION.toString(), msgFactory
            .buildAlterPartitionMessage(partitionEvent.getTable(), before, after).toString());
    event.setDbName(before.getDbName());
    event.setTableName(before.getTableName());
    enqueue(event, partitionEvent);
  }

  /**
   * @param dbEvent database event
   * @throws MetaException
   */
  public void onCreateDatabase(CreateDatabaseEvent dbEvent) throws MetaException {
    Database db = dbEvent.getDatabase();
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.CREATE_DATABASE.toString(), msgFactory
            .buildCreateDatabaseMessage(db).toString());
    event.setDbName(db.getName());
    enqueue(event, dbEvent);
  }

  /**
   * @param dbEvent database event
   * @throws MetaException
   */
  public void onDropDatabase(DropDatabaseEvent dbEvent) throws MetaException {
    Database db = dbEvent.getDatabase();
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.DROP_DATABASE.toString(), msgFactory
            .buildDropDatabaseMessage(db).toString());
    event.setDbName(db.getName());
    enqueue(event, dbEvent);
  }

  /**
   * @param dbEvent alter database event
   * @throws MetaException
   */
  @Override
  public void onAlterDatabase(AlterDatabaseEvent dbEvent) throws MetaException {
    Database oldDb = dbEvent.getOldDatabase();
    Database newDb = dbEvent.getNewDatabase();
    NotificationEvent event =
            new NotificationEvent(0, now(), EventType.ALTER_DATABASE.toString(), msgFactory
                    .buildAlterDatabaseMessage(oldDb, newDb).toString());
    event.setDbName(oldDb.getName());
    enqueue(event, dbEvent);
  }

  /**
   * @param fnEvent function event
   * @throws MetaException
   */
  public void onCreateFunction(CreateFunctionEvent fnEvent) throws MetaException {
    // Sentry doesn't care about this one
  }

  /**
   * @param fnEvent function event
   * @throws MetaException
   */
  public void onDropFunction(DropFunctionEvent fnEvent) throws MetaException {
    // Sentry doesn't care about this one
  }

  /**
   * @param indexEvent index event
   * @throws MetaException
   */
  public void onAddIndex(AddIndexEvent indexEvent) throws MetaException {
    Index index = indexEvent.getIndex();
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.CREATE_INDEX.toString(), msgFactory
            .buildCreateIndexMessage(index).toString());
    event.setDbName(index.getDbName());
    event.setTableName(index.getOrigTableName());
    enqueue(event, indexEvent);
  }

  /**
   * @param indexEvent index event
   * @throws MetaException
   */
  public void onDropIndex(DropIndexEvent indexEvent) throws MetaException {
    Index index = indexEvent.getIndex();
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.DROP_INDEX.toString(), msgFactory
            .buildDropIndexMessage(index).toString());
    event.setDbName(index.getDbName());
    event.setTableName(index.getOrigTableName());
    enqueue(event, indexEvent);
  }

  /**
   * @param indexEvent index event
   * @throws MetaException
   */
  public void onAlterIndex(AlterIndexEvent indexEvent) throws MetaException {
    Index before = indexEvent.getOldIndex();
    Index after = indexEvent.getNewIndex();
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.ALTER_INDEX.toString(), msgFactory
            .buildAlterIndexMessage(before, after).toString());
    event.setDbName(before.getDbName());
    event.setTableName(before.getOrigTableName());
    enqueue(event, indexEvent);
  }

  @Override
  public void onInsert(InsertEvent insertEvent) throws MetaException {
    // Sentry doesn't care about this one
  }

  /**
   * @param partSetDoneEvent
   * @throws MetaException
   */
  public void onLoadPartitionDone(LoadPartitionDoneEvent partSetDoneEvent) throws MetaException {
    // TODO, we don't support this, but we should, since users may create an empty partition and
    // then load data into it.

  }

  private int now() {
    long millis = System.currentTimeMillis();
    millis /= 1000;
    if (millis > Integer.MAX_VALUE) {
      LOG.warn("We've passed max int value in seconds since the epoch, " +
          "all notification times will be the same!");
      return Integer.MAX_VALUE;
    }
    return (int)millis;
  }

  private boolean isVirtualView(Table table) {
    String tableType = table.getTableType();
    return tableType != null && tableType.equalsIgnoreCase(TableType.VIRTUAL_VIEW.name());
  }

  /**
   * Process this notification by adding it to metastore DB.
   *
   * @param event NotificationEvent is the object written to the metastore DB.
   * @param listenerEvent ListenerEvent (from which NotificationEvent was based) used only to set the
   *                      DB_NOTIFICATION_EVENT_ID_KEY_NAME for future reference by other listeners.
   */
  private void enqueue(NotificationEvent event, ListenerEvent listenerEvent) throws MetaException {
      LOG.debug("DbNotificationListener: Processing : {}:{}", event.getEventId(),
          event.getMessage());
      HMSHandler.getMSForConf(hiveConf).addNotificationEvent(event);

      // Set the DB_NOTIFICATION_EVENT_ID for future reference by other listeners.
      if (event.isSetEventId()) {
        listenerEvent.putParameter(
            MetaStoreEventListenerConstants.DB_NOTIFICATION_EVENT_ID_KEY_NAME,
            Long.toString(event.getEventId()));
      }
  }

  private static class CleanerThread extends Thread {
    private RawStore rs;
    private int ttl;
    static private long sleepTime = 60000;

    CleanerThread(HiveConf conf, RawStore rs) {
      super("CleanerThread");
      this.rs = rs;
      setTimeToLive(conf.getTimeVar(HiveConf.ConfVars.METASTORE_EVENT_DB_LISTENER_TTL,
          TimeUnit.SECONDS));
      setDaemon(true);
    }

    @Override
    public void run() {
      while (true) {
        rs.cleanNotificationEvents(ttl);
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          LOG.info("Cleaner thread sleep interupted", e);
        }
      }
    }

    public void setTimeToLive(long configTtl) {
      if (configTtl > Integer.MAX_VALUE) ttl = Integer.MAX_VALUE;
      else ttl = (int)configTtl;
    }

  }

}
