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

package org.apache.hive.jdbc.miniHS2;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.shims.HadoopShims.MiniDFSShim;
import org.apache.hadoop.hive.shims.HadoopShims.MiniMrShim;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hive.service.Service;
import org.apache.hive.service.cli.CLIServiceClient;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.session.HiveSessionHook;
import org.apache.hive.service.cli.session.HiveSessionHookContext;
import org.apache.hive.service.cli.thrift.ThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.apache.hive.service.server.HiveServer2;

import com.google.common.io.Files;

public class MiniHS2 extends AbstarctHiveService {
  private static final String driverName = "org.apache.hive.jdbc.HiveDriver";
  private HiveServer2 hiveServer2 = null;
  private static MiniMrShim mr;
  private static MiniDFSShim dfs;
  private final File baseDir;
  private final Path baseDfsDir;
  private static final AtomicLong hs2Counter = new AtomicLong();

  public MiniHS2(HiveConf hiveConf) throws IOException {
    this(hiveConf, false);
  }

  public MiniHS2(HiveConf hiveConf, boolean useMiniMR) throws IOException {
    super(hiveConf, "localhost", MetaStoreUtils.findFreePort());
    baseDir =  Files.createTempDir();
    FileSystem fs;
    if (useMiniMR) {
      dfs = ShimLoader.getHadoopShims().getMiniDfs(hiveConf, 4, true, null);
      fs = dfs.getFileSystem();
      mr = ShimLoader.getHadoopShims().getMiniMrCluster(hiveConf, 4,
          fs.getUri().toString(), 1);
      setupMiniMrConfig(hiveConf);
      baseDfsDir =  new Path(new Path(fs.getUri()), "/base");
    } else {
      fs = FileSystem.getLocal(hiveConf);
      baseDfsDir = new Path("file://"+ baseDir.getPath());
    }

    fs.mkdirs(baseDfsDir);
    Path wareHouseDir = new Path(baseDfsDir, "warehouse");
    fs.mkdirs(wareHouseDir);
    setWareHouseDir(wareHouseDir.toString());
    String metaStoreURL =  "jdbc:derby:" + baseDir.getAbsolutePath() + File.separator + "test_metastore-" +
        hs2Counter.incrementAndGet() + ";create=true";

    System.setProperty(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname, metaStoreURL);
    hiveConf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, metaStoreURL);
    setPort(MetaStoreUtils.findFreePort());
    hiveConf.setVar(ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST, getHost());
    hiveConf.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_PORT, getPort());
    HiveMetaStore.HMSHandler.resetDefaultDBFlag();

    Path scratchDir = new Path(baseDfsDir, "scratch");
    fs.mkdirs(scratchDir);
    System.setProperty(HiveConf.ConfVars.SCRATCHDIR.varname, scratchDir.toString());
    System.setProperty(HiveConf.ConfVars.LOCALSCRATCHDIR.varname,
        baseDir.getPath() + File.separator + "scratch");
  }

  /**
   * HiveServer2 sessions don't inherit from the global config. As a workaroud,
   * the  test framework is creating a new hive site and injecting that into
   * HiveConf using reflection.
   * @param hiveConf
   * @throws IOException
   */
  private void setupMiniMrConfig(HiveConf hiveConf) throws IOException {
    // store the MR config properties in hiveConf
    mr.setupConfiguration(hiveConf);

    // write out the updated hive configuration
    File hiveSite = new File(baseDir, "hive-site.xml");
    FileOutputStream out = new FileOutputStream(hiveSite );
    hiveConf.writeXml(out);
    out.close();

    // reset the hive-site location to the new file in HiveConf
    resetHiveConfFile(hiveSite.toURI().toURL());
  }

  // set the private static field hiveSiteURL in HiveConf
  private void resetHiveConfFile(URL hiveSiteFile) throws IOException {
    Field f;
    try {
      f = HiveConf.class.getDeclaredField("hiveSiteURL");
    } catch (NoSuchFieldException e) {
      throw new IOException("Failed to modify HiveConf", e);
    } catch (SecurityException e) {
      throw new IOException("Failed to modify HiveConf", e);
    }
    f.setAccessible(true);
    try {
      f.set(null, hiveSiteFile);
    } catch (IllegalArgumentException e) {
      throw new IOException("Failed to modify HiveConf", e);
    } catch (IllegalAccessException e) {
      throw new IOException("Failed to modify HiveConf", e);
    }
  }

  public void start() throws Exception {
    hiveServer2 = new HiveServer2();
    hiveServer2.init(getHiveConf());
    hiveServer2.start();
    waitForStartup();
    setStarted(true);
  }

  public void stop() {
    verifyStarted();
    hiveServer2.stop();
    setStarted(false);
    try {
      if (mr != null) {
        mr.shutdown();
        mr = null;
        resetHiveConfFile(HiveConf.class.getResource("hive-site.xml"));
      }
      if (dfs != null) {
        dfs.shutdown();
        dfs = null;
      }
    } catch (IOException e) {
      // Ignore errors cleaning up miniMR
    } finally {
      clearProperties();
    }
    FileUtils.deleteQuietly(baseDir);
  }

  public CLIServiceClient getServiceClient() {
    verifyStarted();
    return getServiceClientInternal();
  }

  public CLIServiceClient getServiceClientInternal() {
    for (Service service : hiveServer2.getServices()) {
      if (service instanceof ThriftBinaryCLIService) {
        return new ThriftCLIServiceClient((ThriftBinaryCLIService)service);
      }
    }
    throw new IllegalStateException("HS2 not running Thrift service");
  }

  public String getJdbcURL() {
    return "jdbc:hive2://" + getHost() + ":" + getPort() + "/default";
  }

  public static String getJdbcDriverName() {
    return driverName;
  }

  private void waitForStartup() throws Exception {
    int waitTime = 0;
    long startupTimeout = 1000L * 1000000000L;
    CLIServiceClient hs2Client = getServiceClientInternal();
    SessionHandle sessionHandle = null;
    do {
      Thread.sleep(500L);
      waitTime += 500L;
      if (waitTime > startupTimeout) {
        throw new TimeoutException("Couldn't access new HiveServer: " + getJdbcURL());
      }
      try {
        sessionHandle = hs2Client.openSession("foo", "bar");
      } catch (Exception e) {
        // service not started yet
        continue;
      }
      hs2Client.closeSession(sessionHandle);
      break;
    } while (true);
  }

}
