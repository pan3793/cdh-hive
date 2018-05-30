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
package org.apache.hadoop.hive.metastore.datasource;

import com.jolbox.bonecp.BoneCPDataSource;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.SQLException;

public class TestDataSourceProviderFactory {

  private HiveConf conf;

  @Before
  public void init() {
    conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME, "dummyUser");
    conf.setVar(HiveConf.ConfVars.METASTOREPWD, "dummyPass");
    conf.unset(HiveConf.ConfVars.METASTORE_CONNECTION_POOLING_TYPE.varname);
  }

  @Test
  public void testNoDataSourceCreatedWithoutProps() throws SQLException {

    HiveConf.setVar(conf, HiveConf.ConfVars.METASTORE_CONNECTION_POOLING_TYPE, "dummy");

    DataSourceProvider dsp = DataSourceProviderFactory.getDataSourceProvider(conf);
    Assert.assertNull(dsp);
  }

  @Test
  public void testCanCreateDataSourceForSpecificProp() throws SQLException {

    Assert.assertFalse(
            DataSourceProviderFactory.hasProviderSpecificConfigurations(conf));

    conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_POOLING_TYPE, BoneCPDataSourceProvider.BONECP);
    conf.set(BoneCPDataSourceProvider.BONECP + ".dummy.var", "dummy");

    Assert.assertTrue(
            DataSourceProviderFactory.hasProviderSpecificConfigurations(conf));
  }

  @Test
  public void testCreateBoneCpDataSource() throws SQLException {

    conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_POOLING_TYPE, BoneCPDataSourceProvider.BONECP);

    DataSourceProvider dsp = DataSourceProviderFactory.getDataSourceProvider(conf);
    Assert.assertNotNull(dsp);

    DataSource ds = dsp.create(conf);
    Assert.assertTrue(ds instanceof BoneCPDataSource);
  }

  @Test
  public void testSetBoneCpStringProperty() throws SQLException {

    conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_POOLING_TYPE, BoneCPDataSourceProvider.BONECP);
    conf.set(BoneCPDataSourceProvider.BONECP + ".initSQL", "select 1 from dual");

    DataSourceProvider dsp = DataSourceProviderFactory.getDataSourceProvider(conf);
    Assert.assertNotNull(dsp);

    DataSource ds = dsp.create(conf);
    Assert.assertTrue(ds instanceof BoneCPDataSource);
    Assert.assertEquals("select 1 from dual", ((BoneCPDataSource)ds).getInitSQL());
  }

  @Test
  public void testSetBoneCpNumberProperty() throws SQLException {

    conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_POOLING_TYPE, BoneCPDataSourceProvider.BONECP);
    conf.set(BoneCPDataSourceProvider.BONECP + ".acquireRetryDelayInMs", "599");

    DataSourceProvider dsp = DataSourceProviderFactory.getDataSourceProvider(conf);
    Assert.assertNotNull(dsp);

    DataSource ds = dsp.create(conf);
    Assert.assertTrue(ds instanceof BoneCPDataSource);
    Assert.assertEquals(599L, ((BoneCPDataSource)ds).getAcquireRetryDelayInMs());
  }

  @Test
  public void testSetBoneCpBooleanProperty() throws SQLException {

    conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_POOLING_TYPE, BoneCPDataSourceProvider.BONECP);
    conf.set(BoneCPDataSourceProvider.BONECP + ".disableJMX", "true");

    DataSourceProvider dsp = DataSourceProviderFactory.getDataSourceProvider(conf);
    Assert.assertNotNull(dsp);

    DataSource ds = dsp.create(conf);
    Assert.assertTrue(ds instanceof BoneCPDataSource);
    Assert.assertEquals(true, ((BoneCPDataSource)ds).isDisableJMX());
  }

  @Test
  public void testCreateHikariCpDataSource() throws SQLException {

    conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_POOLING_TYPE, HikariCPDataSourceProvider.HIKARI);
    // This is needed to prevent the HikariDataSource from trying to connect to the DB
    conf.set(HikariCPDataSourceProvider.HIKARI + ".initializationFailTimeout", "-1");

    DataSourceProvider dsp = DataSourceProviderFactory.getDataSourceProvider(conf);
    Assert.assertNotNull(dsp);

    DataSource ds = dsp.create(conf);
    Assert.assertTrue(ds instanceof HikariDataSource);
  }

  @Test
  public void testSetHikariCpStringProperty() throws SQLException {

    conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_POOLING_TYPE, HikariCPDataSourceProvider.HIKARI);
    conf.set(HikariCPDataSourceProvider.HIKARI + ".connectionInitSql", "select 1 from dual");
    conf.set(HikariCPDataSourceProvider.HIKARI + ".initializationFailTimeout", "-1");

    DataSourceProvider dsp = DataSourceProviderFactory.getDataSourceProvider(conf);
    Assert.assertNotNull(dsp);

    DataSource ds = dsp.create(conf);
    Assert.assertTrue(ds instanceof HikariDataSource);
    Assert.assertEquals("select 1 from dual", ((HikariDataSource)ds).getConnectionInitSql());
  }

  @Test
  public void testSetHikariCpNumberProperty() throws SQLException {

    conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_POOLING_TYPE, HikariCPDataSourceProvider.HIKARI);
    conf.set(HikariCPDataSourceProvider.HIKARI + ".idleTimeout", "59999");
    conf.set(HikariCPDataSourceProvider.HIKARI + ".initializationFailTimeout", "-1");

    DataSourceProvider dsp = DataSourceProviderFactory.getDataSourceProvider(conf);
    Assert.assertNotNull(dsp);

    DataSource ds = dsp.create(conf);
    Assert.assertTrue(ds instanceof HikariDataSource);
    Assert.assertEquals(59999L, ((HikariDataSource)ds).getIdleTimeout());
  }

  @Test
  public void testSetHikariCpBooleanProperty() throws SQLException {

    conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_POOLING_TYPE, HikariCPDataSourceProvider.HIKARI);
    conf.set(HikariCPDataSourceProvider.HIKARI + ".allowPoolSuspension", "false");
    conf.set(HikariCPDataSourceProvider.HIKARI + ".initializationFailTimeout", "-1");

    DataSourceProvider dsp = DataSourceProviderFactory.getDataSourceProvider(conf);
    Assert.assertNotNull(dsp);

    DataSource ds = dsp.create(conf);
    Assert.assertTrue(ds instanceof HikariDataSource);
    Assert.assertEquals(false, ((HikariDataSource)ds).isAllowPoolSuspension());
  }
  @Test(expected = IllegalArgumentException.class)
  public void testBoneCPConfigCannotBeSet() {
    conf.addToRestrictList(BoneCPDataSourceProvider.BONECP);
    conf.verifyAndSet(BoneCPDataSourceProvider.BONECP + ".disableJMX", "true");
  }

  @Test
  public void testCreateDbCpDataSource() throws SQLException {

    HiveConf.setVar(conf, HiveConf.ConfVars.METASTORE_CONNECTION_POOLING_TYPE, DbCPDataSourceProvider.DBCP);

    DataSourceProvider dsp = DataSourceProviderFactory.getDataSourceProvider(conf);
    Assert.assertNotNull(dsp);

    DataSource ds = dsp.create(conf);
    Assert.assertTrue(ds instanceof PoolingDataSource);
  }

  @Test
  public void testHasProviderSpecificConfigurationBonecp() throws SQLException {

    HiveConf.setVar(conf, HiveConf.ConfVars.METASTORE_CONNECTION_POOLING_TYPE, BoneCPDataSourceProvider.BONECP);

    Assert.assertFalse(DataSourceProviderFactory.hasProviderSpecificConfigurations(conf));

    conf.set("dbcp.dummyConf", "dummyValue");
    Assert.assertFalse(DataSourceProviderFactory.hasProviderSpecificConfigurations(conf));

    conf.set("hikaricp.dummyConf", "dummyValue");
    Assert.assertFalse(DataSourceProviderFactory.hasProviderSpecificConfigurations(conf));

    conf.set("bonecp.dummyConf", "dummyValue");
    Assert.assertTrue(DataSourceProviderFactory.hasProviderSpecificConfigurations(conf));

  }

  @Test
  public void testHasProviderSpecificConfigurationHikaricp() throws SQLException {

    HiveConf.setVar(conf, HiveConf.ConfVars.METASTORE_CONNECTION_POOLING_TYPE, HikariCPDataSourceProvider.HIKARI);

    Assert.assertFalse(DataSourceProviderFactory.hasProviderSpecificConfigurations(conf));

    conf.set("dbcp.dummyConf", "dummyValue");
    Assert.assertFalse(DataSourceProviderFactory.hasProviderSpecificConfigurations(conf));

    conf.set("bonecp.dummyConf", "dummyValue");
    Assert.assertFalse(DataSourceProviderFactory.hasProviderSpecificConfigurations(conf));

    conf.set("hikaricp.dummyConf", "dummyValue");
    Assert.assertTrue(DataSourceProviderFactory.hasProviderSpecificConfigurations(conf));

  }

  @Test
  public void testHasProviderSpecificConfigurationDbcp() throws SQLException {

    HiveConf.setVar(conf, HiveConf.ConfVars.METASTORE_CONNECTION_POOLING_TYPE, DbCPDataSourceProvider.DBCP);

    Assert.assertFalse(DataSourceProviderFactory.hasProviderSpecificConfigurations(conf));

    conf.set("hikaricp.dummyConf", "dummyValue");
    Assert.assertFalse(DataSourceProviderFactory.hasProviderSpecificConfigurations(conf));

    conf.set("bonecp.dummyConf", "dummyValue");
    Assert.assertFalse(DataSourceProviderFactory.hasProviderSpecificConfigurations(conf));

    conf.set("dbcp.dummyConf", "dummyValue");
    Assert.assertTrue(DataSourceProviderFactory.hasProviderSpecificConfigurations(conf));

  }
}
