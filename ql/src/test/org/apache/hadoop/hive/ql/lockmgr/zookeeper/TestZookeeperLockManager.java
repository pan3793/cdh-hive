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

package org.apache.hadoop.hive.ql.lockmgr.zookeeper;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestZookeeperLockManager {

  private HiveConf conf;

  @Before
  public void setup() {
    conf = new HiveConf();
  }



  @Test
  public void testGetQuorumServers() {
    conf.setVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_QUORUM, "node1");
    conf.setVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_CLIENT_PORT, "9999");
    Assert.assertEquals("node1:9999", ZooKeeperHiveLockManager.getQuorumServers(conf));

    conf.setVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_QUORUM, "node1,node2,node3");
    conf.setVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_CLIENT_PORT, "9999");
    Assert.assertEquals("node1:9999,node2:9999,node3:9999", ZooKeeperHiveLockManager.getQuorumServers(conf));

    conf.setVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_QUORUM, "node1:5666,node2,node3");
    conf.setVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_CLIENT_PORT, "9999");
    Assert.assertEquals("node1:5666,node2:9999,node3:9999", ZooKeeperHiveLockManager.getQuorumServers(conf));
  }
}
