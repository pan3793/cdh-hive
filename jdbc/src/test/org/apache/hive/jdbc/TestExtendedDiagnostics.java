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

package org.apache.hive.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestExtendedDiagnostics {

  // Mock Connection implementation to force getInfo
  public static class HiveTestConnection extends HiveConnection {
    public HiveTestConnection(String uri, Properties info) throws SQLException {
      super(uri, info);
    }

    // force GetInfo for extended error cases
    protected boolean needExtendedErrorFromServer() {
      return true;
    }
  }

  // Mock Driver to use the mock connection
  public static class HiveTestDriver extends HiveDriver implements Driver {
    public Connection connect(String url, Properties info) throws SQLException {
      return new HiveTestConnection(url, info);
    }
  }

  public static class QueryBlockHook implements ExecuteWithHookContext {

    private static boolean blockHook = true;

    // hook that waits till it's unblocked
    public void run(HookContext hookContext) throws Exception {
      while (blockHook) {
        try {
          Thread.sleep(200);
        } catch (InterruptedException e) {
          break;
        }
      }
      throw new IllegalStateException("Mock Hook failures");
    }

    public static void setBlock() {
      blockHook = true;
    }

    public static void clearBlock() {
      blockHook = false;
    }
  }

  private static MiniHS2 miniHS2 = null;
  private static HiveConf conf = new HiveConf();
  private Connection hs2Conn = null;

  @BeforeClass
  public static void beforeTest() throws Exception {
    // register the mock driver
    Class.forName(HiveTestDriver.class.getName());
    DriverManager.deregisterDriver(DriverManager.getDriver("jdbc:hive2://"));
    DriverManager.registerDriver(new HiveTestDriver());

    miniHS2 = new MiniHS2(conf);
  }

  @Before
  public void setUp() throws Exception {
    miniHS2.start();
    hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL());
  }

  @After
  public void tearDown() throws Exception {
    if (hs2Conn != null) {
      hs2Conn.close();
    }
    if (miniHS2 != null && miniHS2.isStarted()) {
      miniHS2.stop();
    }
  }

  @Test
  public void testServerName() throws Exception {
    assertEquals("Hive", hs2Conn.getMetaData().getDatabaseProductName());
  }

  /**
   * setup mock hook that will wait till signled and throw assertion
   * The query will be submitted for background execution and block
   * Test then unblocks the hook which causes the query to fail on server
   * The subsequent fetch fails. The test verifies if the failures triggered
   * the mock JDBC driver to retrieve additional error via GetLog().
   * @throws Exception
   */
  @Test
  public void testExtendedErrorRetrieval() throws Exception {
    Statement stmt = hs2Conn.createStatement();

    stmt.execute("DROP TABLE IF EXISTS tab1");
    stmt.execute("CREATE TABLE tab1(id INT)");

    // set block hook
    stmt.execute("set hive.exec.post.hooks = " + QueryBlockHook.class.getName());
    stmt.execute("set hive.server2.blocking.query=false");

    QueryBlockHook.setBlock();
    String queryStr = "SELECT id FROM tab1";
    ResultSet res = stmt.executeQuery(queryStr);
    QueryBlockHook.clearBlock();
    try {
      res.next();
      fail("next() should fail due to mock hook error");
    } catch (SQLException e) {
      // verify that the GetLog got called
      assertTrue(e.getMessage(), e.getMessage().contains(queryStr));
    }
  }

}
