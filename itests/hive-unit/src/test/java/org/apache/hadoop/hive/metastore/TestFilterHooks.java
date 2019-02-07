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
package org.apache.hadoop.hive.metastore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.UtilsForTest;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFilterHooks {

  public static class DummyMetaStoreFilterHookImpl extends DefaultMetaStoreFilterHookImpl {
    public static boolean blockResults = false;

    public DummyMetaStoreFilterHookImpl(HiveConf conf) {
      super(conf);
    }

    @Override
    public List<String> filterDatabases(List<String> dbList) throws MetaException  {
      if (blockResults) {
        return new ArrayList<String>();
      }
      return super.filterDatabases(dbList);
    }

    @Override
    public Database filterDatabase(Database dataBase) throws NoSuchObjectException {
      if (blockResults) {
        throw new NoSuchObjectException("Blocked access");
      }
      return super.filterDatabase(dataBase);
    }

    @Override
    public List<String> filterTableNames(String dbName, List<String> tableList) throws MetaException {
      if (blockResults) {
        return new ArrayList<String>();
      }
      return super.filterTableNames(dbName, tableList);
    }

    @Override
    public Table filterTable(Table table) throws NoSuchObjectException {
      if (blockResults) {
        throw new NoSuchObjectException("Blocked access");
      }
      return super.filterTable(table);
    }

    @Override
    public List<Table> filterTables(List<Table> tableList) throws MetaException {
      if (blockResults) {
        return new ArrayList<Table>();
      }
      return super.filterTables(tableList);
    }

    @Override
    public List<Partition> filterPartitions(List<Partition> partitionList) throws MetaException {
      if (blockResults) {
        return new ArrayList<Partition>();
      }
      return super.filterPartitions(partitionList);
    }

    @Override
    public List<PartitionSpec> filterPartitionSpecs(
        List<PartitionSpec> partitionSpecList) throws MetaException {
      if (blockResults) {
        return new ArrayList<PartitionSpec>();
      }
      return super.filterPartitionSpecs(partitionSpecList);
    }

    @Override
    public Partition filterPartition(Partition partition) throws NoSuchObjectException {
      if (blockResults) {
        throw new NoSuchObjectException("Blocked access");
      }
      return super.filterPartition(partition);
    }

    @Override
    public List<String> filterPartitionNames(String dbName, String tblName,
        List<String> partitionNames) throws MetaException {
      if (blockResults) {
        return new ArrayList<String>();
      }
      return super.filterPartitionNames(dbName, tblName, partitionNames);
    }

    @Override
    public Index filterIndex(Index index) throws NoSuchObjectException {
      if (blockResults) {
        throw new NoSuchObjectException("Blocked access");
      }
      return super.filterIndex(index);
    }

    @Override
    public List<String> filterIndexNames(String dbName, String tblName,
        List<String> indexList) throws MetaException {
      if (blockResults) {
        return new ArrayList<String>();
      }
      return super.filterIndexNames(dbName, tblName, indexList);
    }

    @Override
    public List<Index> filterIndexes(List<Index> indexeList) throws MetaException {
      if (blockResults) {
        return new ArrayList<Index>();
      }
      return super.filterIndexes(indexeList);
    }
  }

  private static String DBNAME1 = "testdb1";
  private static String DBNAME2 = "testdb2";
  private static final String TAB1 = "tab1";
  private static final String TAB2 = "tab2";
  private static final String INDEX1 = "idx1";
  private static HiveConf hiveConf;
  private static HiveMetaStoreClient msc;
  private static IDriver driver;

  @Before
  public void setUp() throws Exception {
    DummyMetaStoreFilterHookImpl.blockResults = false;

    hiveConf = new HiveConf(TestFilterHooks.class);
    hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
    hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    hiveConf.setVar(ConfVars.METASTORE_FILTER_HOOK, DummyMetaStoreFilterHookImpl.class.getName());
    UtilsForTest.setNewDerbyDbLocation(hiveConf, TestFilterHooks.class.getSimpleName());
  }

  private void createEnv(HiveConf conf) throws Exception {
    MetaStoreUtils.startMetaStoreWithRetry(hiveConf);
    SessionState.start(new CliSessionState(conf));
    msc = new HiveMetaStoreClient(conf);
    driver = DriverFactory.newDriver(conf);

    driver.run("drop database if exists " + DBNAME1  + " cascade");
    driver.run("drop database if exists " + DBNAME2  + " cascade");
    driver.run("create database " + DBNAME1);
    driver.run("create database " + DBNAME2);
    driver.run("use " + DBNAME1);
    driver.run("create table " + DBNAME1 + "." + TAB1 + " (id int, name string)");
    driver.run("create table " + TAB2 + " (id int) partitioned by (name string)");
    driver.run("ALTER TABLE " + TAB2 + " ADD PARTITION (name='value1')");
    driver.run("ALTER TABLE " + TAB2 + " ADD PARTITION (name='value2')");
    driver.run("CREATE INDEX " + INDEX1 + " on table " + TAB1 + "(id) AS 'COMPACT' WITH DEFERRED REBUILD");
  }

  @After
  public void tearDown() throws Exception {
    DummyMetaStoreFilterHookImpl.blockResults = false;
    driver.run("drop database if exists " + DBNAME1  + " cascade");
    driver.run("drop database if exists " + DBNAME2  + " cascade");
    driver.close();
    driver.destroy();
    if ( msc != null ) {
      msc.close();
    }
  }

  /**
   * By default, the HMS Client side filtering is enabled
   * Disable the HMS server side filtering in order to see HMS client filtering behavior
   * @throws Exception
   */
  @Test
  public void testHMSClientWithFilter() throws Exception {
    DummyMetaStoreFilterHookImpl.blockResults = false;
    HiveConf.setBoolVar(hiveConf, ConfVars.METASTORE_CLIENT_FILTER_ENABLED, true);
    HiveConf.setBoolVar(hiveConf, ConfVars.METASTORE_SERVER_FILTER_ENABLED, false);
    DBNAME1 = "db_testHMSClientWithFilter_1";
    DBNAME2 = "db_testHMSClientWithFilter_2";
    createEnv(hiveConf);

    testDefaultFilter();
    testDummyFilterForTables();
    testDummyFilterForDb();
    testDummyFilterForPartition();
    testDummyFilterForIndex();
  }

  /**
   * The default configuration should be disable filtering at HMS server
   * Disable the HMS client side filtering in order to see HMS server filtering behavior
   * @throws Exception
   */
  @Test
  public void testHMSServerWithoutFilter() throws Exception {
    DummyMetaStoreFilterHookImpl.blockResults = true;
    HiveConf.setBoolVar(hiveConf, ConfVars.METASTORE_CLIENT_FILTER_ENABLED, false);
    HiveConf.setBoolVar(hiveConf, ConfVars.METASTORE_SERVER_FILTER_ENABLED, false);
    DBNAME1 = "db_testHMSServerWithoutFilter_1";
    DBNAME2 = "db_testHMSServerWithoutFilter_2";
    createEnv(hiveConf);

    testDefaultFilter();
  }

  /**
   * Enable the HMS server side filtering
   * Disable the HMS client side filtering in order to see HMS server filtering behavior
   * @throws Exception
   */
  @Test
  public void testHMSServerWithFilter() throws Exception {
    DummyMetaStoreFilterHookImpl.blockResults = true;
    HiveConf.setBoolVar(hiveConf, ConfVars.METASTORE_CLIENT_FILTER_ENABLED, false);
    HiveConf.setBoolVar(hiveConf, ConfVars.METASTORE_SERVER_FILTER_ENABLED, true);
    DBNAME1 = "db_testHMSServerWithFilter_1";
    DBNAME2 = "db_testHMSServerWithFilter_2";
    createEnv(hiveConf);

    testFilterForDb(true);
    testFilterForTables(true);
    testFilterForPartition(true);
  }

  protected void testFilterForDb(boolean filterAtServer) throws Exception {

    // Skip this call when testing filter hook at HMS server because HMS server calls authorization
    // API for getDatabase(), and does not call filter hook
    if (!filterAtServer) {
      try {
        assertNotNull(msc.getDatabase(DBNAME1));
        fail("getDatabase() should fail with blocking mode");
      } catch (NoSuchObjectException e) {
        // Excepted
      }
    }

    assertEquals(0, msc.getDatabases("*").size());
    assertEquals(0, msc.getAllDatabases().size());
    assertEquals(0, msc.getDatabases(DBNAME1).size());
  }

  protected void testFilterForTables(boolean filterAtServer) throws Exception {
    // Skip this call when testing filter hook at HMS server because HMS server calls authorization
    // API for getTable(), and does not call filter hook
    if (!filterAtServer) {
      try {
        msc.getTable(DBNAME1, TAB1);
        fail("getTable() should fail with blocking mode");
      } catch (NoSuchObjectException e) {
        // Excepted
      }
    }

    assertEquals(0, msc.getTables(DBNAME1, "*").size());
    assertEquals(0, msc.getAllTables(DBNAME1).size());
    assertEquals(0, msc.getTables(DBNAME1, TAB2).size());
  }

  protected void testFilterForPartition(boolean filterAtServer) throws Exception {
    try {
      assertNotNull(msc.getPartition(DBNAME1, TAB2, "name=value1"));
      fail("getPartition() should fail with blocking mode");
    } catch (NoSuchObjectException e) {
      // Excepted
    }

    if (filterAtServer) {
      // at HMS server, the table of the partitions should be filtered out and result in
      // NoSuchObjectException
      try {
        msc.getPartitionsByNames(DBNAME1, TAB2,
            Lists.newArrayList("name=value1")).size();
        fail("getPartitionsByNames() should fail with blocking mode at server side");
      } catch (NoSuchObjectException e) {
        // Excepted
      }
    } else {
      // at HMS client, we cannot filter the table of the partitions due to
      // HIVE-21227: HIVE-20776 causes view access regression
      assertEquals(0, msc.getPartitionsByNames(DBNAME1, TAB2,
          Lists.newArrayList("name=value1")).size());
    }

  }

  private void testDefaultFilter() throws Exception {
    assertNotNull(msc.getTable(DBNAME1, TAB1));
    assertEquals(3, msc.getTables(DBNAME1, "*").size());
    assertEquals(3, msc.getAllTables(DBNAME1).size());
    assertEquals(1, msc.getTables(DBNAME1, TAB2).size());
    assertEquals(0, msc.getAllTables(DBNAME2).size());

    assertNotNull(msc.getDatabase(DBNAME1));
    assertEquals(3, msc.getDatabases("*").size());
    assertEquals(3, msc.getAllDatabases().size());
    assertEquals(1, msc.getDatabases(DBNAME1).size());

    assertNotNull(msc.getPartition(DBNAME1, TAB2, "name=value1"));
    assertEquals(1, msc.getPartitionsByNames(DBNAME1, TAB2, Lists.newArrayList("name=value1")).size());

    assertNotNull(msc.getIndex(DBNAME1, TAB1, INDEX1));
  }

  private void testDummyFilterForTables() throws Exception {
    DummyMetaStoreFilterHookImpl.blockResults = true;
    try {
      msc.getTable(DBNAME1, TAB1);
      fail("getTable() should fail with blocking mode");
    } catch (NoSuchObjectException e) {
      // Excepted
    }
    assertEquals(0, msc.getTables(DBNAME1, "*").size());
    assertEquals(0, msc.getAllTables(DBNAME1).size());
    assertEquals(0, msc.getTables(DBNAME1, TAB2).size());
  }

  private void testDummyFilterForDb() throws Exception {
    DummyMetaStoreFilterHookImpl.blockResults = true;
    try {
      assertNotNull(msc.getDatabase(DBNAME1));
      fail("getDatabase() should fail with blocking mode");
    } catch (NoSuchObjectException e) {
        // Excepted
    }
    assertEquals(0, msc.getDatabases("*").size());
    assertEquals(0, msc.getAllDatabases().size());
    assertEquals(0, msc.getDatabases(DBNAME1).size());
  }

  private void testDummyFilterForPartition() throws Exception {
    DummyMetaStoreFilterHookImpl.blockResults = true;
    testFilterForPartition(false);
  }

  private void testDummyFilterForIndex() throws Exception {
    DummyMetaStoreFilterHookImpl.blockResults = true;
    try {
      assertNotNull(msc.getIndex(DBNAME1, TAB1, INDEX1));
      fail("getPartition() should fail with blocking mode");
    } catch (NoSuchObjectException e) {
      // Excepted
    }
  }

}
