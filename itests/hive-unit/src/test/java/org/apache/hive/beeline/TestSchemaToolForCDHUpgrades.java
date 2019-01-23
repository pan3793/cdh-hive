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

package org.apache.hive.beeline;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.PrintStream;
import java.sql.Connection;
import java.util.Random;

/**
 * Cloudera specific test to test CDH specific schema upgrades using schemaTool
 */
public class TestSchemaToolForCDHUpgrades {
  private HiveSchemaTool schemaTool;
  private Connection conn;
  private HiveConf hiveConf;
  private String testMetastoreDB;
  private PrintStream errStream;
  private PrintStream outStream;

  @Before
  public void setUp() throws Exception {
    testMetastoreDB = System.getProperty("java.io.tmpdir") +
        File.separator + "test_metastore-" + new Random().nextInt();
    System.setProperty(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname,
        "jdbc:derby:" + testMetastoreDB + ";create=true");
    hiveConf = new HiveConf(this.getClass());
    schemaTool = new HiveSchemaTool(
        System.getProperty("test.tmp.dir", "target/tmp"), hiveConf, "derby");
    schemaTool.setUserName(
        schemaTool.getHiveConf().get(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME.varname));
    schemaTool.setPassWord(ShimLoader.getHadoopShims().getPassword(schemaTool.getHiveConf(),
        HiveConf.ConfVars.METASTOREPWD.varname));
    System.setProperty("beeLine.system.exit", "true");
    errStream = System.err;
    outStream = System.out;
    conn = schemaTool.getConnectionToMetastore(false);
  }

  @After
  public void tearDown() throws Exception {
    File metaStoreDir = new File(testMetastoreDB);
    if (metaStoreDir.exists()) {
      FileUtils.forceDeleteOnExit(metaStoreDir);
    }
    System.setOut(outStream);
    System.setErr(errStream);
    if (conn != null) {
      conn.close();
    }
  }

  /**
   * Init to the latest schema and make sure that its compatible with the current hiveVersion
   */
  @Test
  public void testLatestCDHVersionCompat() throws HiveMetaException {
    schemaTool.doInit();
    schemaTool.verifySchemaVersion();
  }

  @Test
  public void testCDHUpgradePath() throws HiveMetaException {
    // CDH 5.12.0 is the earliest CDH version with CDH specific schema patch
    schemaTool.doInit("1.1.0-cdh5.12.0");
    schemaTool.doUpgrade();
    schemaTool.verifySchemaVersion();
  }
}
