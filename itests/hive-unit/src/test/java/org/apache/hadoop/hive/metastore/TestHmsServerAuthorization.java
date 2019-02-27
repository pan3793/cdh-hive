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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.List;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.UtilsForTest;

import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test the filtering behavior at HMS client and HMS server. The configuration at each test
 * changes, and therefore HMS client and server are created for each test case
 */
public class TestHmsServerAuthorization {

  /**
   * Implementation of MetaStorePreEventListener that throw MetaException when configuration in
   * its function onEvent()
   */
  public static class DummyAuthorizationListenerImpl extends MetaStorePreEventListener {
    private static volatile boolean throwExceptionAtCall = false;
    public DummyAuthorizationListenerImpl(Configuration config) {
      super(config);
    }

    @Override
    public void onEvent(PreEventContext context)
        throws MetaException, NoSuchObjectException, InvalidOperationException {
      if (throwExceptionAtCall) {
        throw new MetaException("Authorization fails");
      }
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

  @BeforeClass
  public static void setUp() throws Exception {
    // make sure env setup works
    TestHmsServerAuthorization.DummyAuthorizationListenerImpl.throwExceptionAtCall = false;

    hiveConf = new HiveConf(TestFilterHooks.class);
    hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
    hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    hiveConf.setVar(ConfVars.METASTORE_PRE_EVENT_LISTENERS,
        DummyAuthorizationListenerImpl.class.getName());
    HiveConf.setBoolVar(hiveConf, ConfVars.METASTORE_CLIENT_FILTER_ENABLED, false);
    HiveConf.setBoolVar(hiveConf, ConfVars.METASTORE_SERVER_FILTER_ENABLED, false);
    UtilsForTest.setNewDerbyDbLocation(hiveConf, TestFilterHooks.class.getSimpleName());
  }

  private static void createEnv(HiveConf conf) throws Exception {
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

  @AfterClass
  public static void tearDown() throws Exception {
    // make sure tear down works
    TestHmsServerAuthorization.DummyAuthorizationListenerImpl.throwExceptionAtCall = false;

    driver.run("drop database if exists " + DBNAME1  + " cascade");
    driver.run("drop database if exists " + DBNAME2  + " cascade");
    driver.close();
    driver.destroy();
    if ( msc != null ) {
      msc.close();
    }
  }

  /**
   * Test the pre-event listener is called in function get_fields at HMS server.
   * @throws Exception
   */
  @Test
  public void testGetFields() throws Exception {
    DBNAME1 = "db_test_get_fields_1";
    DBNAME2 = "db_test_get_fields_2";
    createEnv(hiveConf);

    // enable throwing exception, so we can check pre-envent listener is called
    TestHmsServerAuthorization.DummyAuthorizationListenerImpl.throwExceptionAtCall = true;

    try {
      List<FieldSchema> tableSchema = msc.getFields(DBNAME1, TAB1);
      fail("getFields() should fail with throw exception mode at server side");
    } catch (MetaException ex) {
      boolean isMessageAuthorization = ex.getMessage().contains("Authorization fails");
      assertEquals(true, isMessageAuthorization);
    }
  }
}