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

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;

import junit.framework.Assert;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.session.HiveSessionHook;
import org.apache.hive.service.cli.session.HiveSessionHookContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestNoSaslAuth {
  private static final Logger LOG = LoggerFactory.getLogger(TestNoSaslAuth.class);
  private static MiniHS2 miniHS2;
  private static String sessionUserName;

  public static class NoSaslSessionHook implements HiveSessionHook {
    private static boolean checkUser;
    private static Throwable error;

    @Override
    public void run(HiveSessionHookContext sessionHookContext)
        throws HiveSQLException {
      try {
        if (checkUser) {
          Assert.assertEquals(sessionHookContext.getSessionUser(), sessionUserName);
        }
      } catch (Throwable t) {
        error = t;
        LOG.error("Error in NoSaslSessionHook: " + error, error);
      }
    }
  }

  private Connection hs2Conn = null;

  @BeforeClass
  public static void beforeTest() throws Exception {
    Class.forName(MiniHS2.getJdbcDriverName());
    HiveConf conf = new HiveConf();
    conf.set(ConfVars.HIVE_SERVER2_AUTHENTICATION.varname, "NOSASL");
    conf.set(ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    conf.set(ConfVars.HIVE_SERVER2_ENABLE_DOAS.varname, "false");
    conf.setVar(ConfVars.HIVE_SERVER2_SESSION_HOOK,
        NoSaslSessionHook.class.getName());
    miniHS2 = new MiniHS2(conf);
    miniHS2.start(new HashMap<String, String>());
  }

  @Before
  public void setUp() throws Exception {
    // enable the hook check after the server startup, 
    NoSaslSessionHook.checkUser = true;
    NoSaslSessionHook.error = null;
  }

  @After
  public void tearDown() throws Exception {
    hs2Conn.close();
  }

  @AfterClass
  public static void afterTest() throws Exception {
    if (miniHS2.isStarted())
      miniHS2.stop();
  }

  /**
   * Initiate a non-sasl connection. The session hook will verfiy the user name
   * set correctly
   * 
   * @throws Exception
   */
  @Test
  public void testConnection() throws Throwable {
    sessionUserName = "user1";
    hs2Conn = DriverManager.getConnection(
        miniHS2.getJdbcURL() + ";auth=noSasl", sessionUserName, "foo");
    Throwable error = NoSaslSessionHook.error;
    if (error != null) {
      throw error;
    }
  }
}
