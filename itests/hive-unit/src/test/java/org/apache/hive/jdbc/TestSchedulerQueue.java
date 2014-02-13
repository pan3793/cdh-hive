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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.jdbc.miniHS2.MiniHS2;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSchedulerQueue {

  private MiniHS2 miniHS2 = null;
  private static HiveConf conf = new HiveConf();
  private Connection hs2Conn = null;
  private String dataFileDir = conf.get("test.data.files");

  @BeforeClass
  public static void beforeTest() throws Exception {
    Class.forName(MiniHS2.getJdbcDriverName());
  }

  @Before
  public void setUp() throws Exception {
    DriverManager.setLoginTimeout(0);
    if (!System.getProperty("test.data.files", "").isEmpty()) {
      dataFileDir = System.getProperty("test.data.files");
    }
    dataFileDir = dataFileDir.replace('\\', '/').replace("c:", "");
    conf.set("mapred.jobtracker.taskScheduler", "org.apache.hadoop.mapred.FairScheduler");
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    miniHS2 = new MiniHS2(conf, true);
    miniHS2.start(new HashMap<String, String>());
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

  /***
   * Test SSL default queue mapping
   * @throws Exception
   */
  @Test
  public void testFairSchedulerQueueMapping() throws Exception {
    hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL(), "user1", "bar");
    verifyProperty("mapreduce.framework.name", "yarn");
    verifyProperty("mapred.jobtracker.taskScheduler", "org.apache.hadoop.mapred.FairScheduler");
    verifyProperty("mapred.job.queue.name", "root.user1");
  }

  @Test
  public void testFairSchedulerQueueMappingDisabled() throws Exception {
    miniHS2.setConfProperty(HiveConf.ConfVars.HIVE_SERVER2_MAP_FAIR_SCHEDULER_QUEUE.varname,
        "false");
    hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL(), "user1", "bar");
    verifyProperty("mapred.job.queue.name", "default");
    miniHS2.setConfProperty(HiveConf.ConfVars.HIVE_SERVER2_MAP_FAIR_SCHEDULER_QUEUE.varname,
        "true");
  }

  /**
   * Verify if the given property contains the expected value
   * @param propertyName
   * @param expectedValue
   * @throws Exception
   */
  private void verifyProperty(String propertyName, String expectedValue) throws Exception {
    Statement stmt = hs2Conn .createStatement();
    ResultSet res = stmt.executeQuery("set " + propertyName);
    assertTrue(res.next());
    String results[] = res.getString(1).split("=");
    assertEquals(expectedValue, results[1]);
  }

}
