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

import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.shims.ShimLoader;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

public interface DataSourceProvider {

  /**
   * @param hdpConfig
   * @return the new connection pool
   */
  DataSource create(Configuration hdpConfig) throws SQLException;

  /**
   * BoneCp has a bug which causes closed connections to be returned to the pool
   * under certain conditions. (HIVE-11915)
   * @return true if the factory creates BoneCp pools which need "special attention"
   */
  boolean mayReturnClosedConnection();

  /**
   * Get the declared pooling type string. This is used to check against the constant in
   * config options.
   * @return The pooling type string associated with the data source.
   */
  String getPoolingType();

  /**
   * @param hdpConfig
   * @return subset of properties prefixed by a connection pool specific substring
   */
  static Properties getPrefixedProperties(Configuration hdpConfig, String factoryPrefix) {
    Properties dataSourceProps = new Properties();
    Iterables.filter(
        hdpConfig, (entry -> entry.getKey() != null && entry.getKey().startsWith(factoryPrefix)))
        .forEach(entry -> dataSourceProps.put(entry.getKey(), entry.getValue()));
    return dataSourceProps;
  }

  static String getMetastoreJdbcUser(Configuration conf) {
    return HiveConf.getVar(conf, HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME);
  }

  static String getMetastoreJdbcPasswd(Configuration conf) throws SQLException {
    try {
      return ShimLoader.getHadoopShims().getPassword(conf, HiveConf.ConfVars.METASTOREPWD.varname);
    } catch (IOException err) {
      throw new SQLException("Error getting metastore password", err);
    }
  }

  static String getMetastoreJdbcDriverUrl(Configuration conf) throws SQLException {
    return HiveConf.getVar(conf, HiveConf.ConfVars.METASTORECONNECTURLKEY);
  }

}
