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

package org.apache.hive.service.cli;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hive.service.cli.thrift.EmbeddedThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.junit.Test;

public class TestScratchDir {
  @Test
  public void testScratchDirs() throws Exception {
    String scratchDirStr = "/tmp/foobar";
    System.setProperty("hive.exec.scratchdir", scratchDirStr);
    ThriftCLIService service = new EmbeddedThriftBinaryCLIService();
    ThriftCLIServiceClient client = new ThriftCLIServiceClient(service);
    final Path scratchDir = new Path(scratchDirStr);
    FileSystem fs = scratchDir.getFileSystem(new Configuration());
    assertTrue(fs.exists(scratchDir));

    FileStatus[] fStatus = fs.globStatus(scratchDir.getParent());
    assertEquals(new FsPermission((short)0777), fStatus[0].getPermission());
    service.stop();
    fs.delete(scratchDir, true);
  }

}
