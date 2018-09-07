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

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.common.StatsSetupConst.COLUMN_STATS_ACCURATE;
import static org.apache.hadoop.hive.common.StatsSetupConst.NUM_FILES;
import static org.apache.hadoop.hive.common.StatsSetupConst.TOTAL_SIZE;
import static org.apache.hadoop.hive.metastore.MetaStoreUtils.updateTableStatsSlow;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestMetaStoreUtils {
  private static final String DB_NAME = "db1";
  private static final String TABLE_NAME = "tbl1";
  private static final String SERDE_LIB = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";


  private final Map<String, String> paramsWithStats = ImmutableMap.of(
      NUM_FILES, "1",
      TOTAL_SIZE, "2"
  );

  private Database db;

  public TestMetaStoreUtils() {
    db = new Database(DB_NAME, "", "/", null);
  }



  @Test
  public void testColumnsIncluded() {
    FieldSchema col1 = new FieldSchema("col1", "string", "col1 comment");
    FieldSchema col2 = new FieldSchema("col2", "string", "col2 comment");
    FieldSchema col3 = new FieldSchema("col3", "string", "col3 comment");
    Assert.assertTrue(MetaStoreUtils.columnsIncluded(Arrays.asList(col1), Arrays.asList(col1)));
    Assert.assertTrue(MetaStoreUtils.columnsIncluded(Arrays.asList(col1, col2), Arrays.asList(col1, col2)));
    Assert.assertTrue(MetaStoreUtils.columnsIncluded(Arrays.asList(col1, col2), Arrays.asList(col2, col1)));
    Assert.assertTrue(MetaStoreUtils.columnsIncluded(Arrays.asList(col1, col2), Arrays.asList(col1, col2, col3)));
    Assert.assertTrue(MetaStoreUtils.columnsIncluded(Arrays.asList(col1, col2), Arrays.asList(col3, col2, col1)));
    Assert.assertFalse(MetaStoreUtils.columnsIncluded(Arrays.asList(col1, col2), Arrays.asList(col1)));
  }

  /**
   * Verify that updateTableStatsSlow really updates table statistics.
   * The test does the following:
   * <ol>
   *   <li>Create database</li>
   *   <li>Create unpartitioned table</li>
   *   <li>Create unpartitioned table which has params</li>
   *   <li>Call updateTableStatsSlow with arguments which should cause stats calculation</li>
   *   <li>Verify table statistics using mocked warehouse</li>
   *   <li>Create table which already have stats</li>
   *   <li>Call updateTableStatsSlow forcing stats recompute</li>
   *   <li>Verify table statistics using mocked warehouse</li>
   * </ol>
   */
  @Test
  public void testUpdateTableStatsSlow_statsUpdated() throws TException {
    long fileLength = 5;

    // Create database and table
    Table tbl = new Table();
    tbl.setDbName(DB_NAME);
    tbl.setTableName(TABLE_NAME);
    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(Collections.singletonList(new FieldSchema("id", "int", "")));
    sd.setLocation("/tmp");
    tbl.setSd(sd);
    tbl.setParameters(new HashMap<String, String>());

    // Set up mock warehouse
    FileStatus fs1 = getFileStatus(1, true, 2, 3, 4, "/tmp/0");
    FileStatus fs2 = getFileStatus(fileLength, false, 3, 4, 5, "/tmp/1");
    FileStatus fs3 = getFileStatus(fileLength, false, 3, 4, 5, "/tmp/1");
    FileStatus[] fileStatus = {fs1, fs2, fs3};
    Warehouse wh = mock(Warehouse.class);
    when(wh.getFileStatusesForUnpartitionedTable(db, tbl)).thenReturn(fileStatus);

    Map<String, String> expected = ImmutableMap.of(NUM_FILES, "2",
        TOTAL_SIZE, String.valueOf(2 * fileLength),
        "numRows", "-1",
        "rawDataSize", "-1",
        COLUMN_STATS_ACCURATE, "false"
    );
    updateTableStatsSlow(db, tbl, wh, false, false);
    assertThat(tbl.getParameters(), is(expected));

    // Verify that when stats are already present and forceRecompute is specified they are recomputed
    Table tbl1 = new Table();
    tbl1.setDbName(DB_NAME);
    tbl1.setTableName(TABLE_NAME);
    tbl1.setSd(sd);
    tbl1.setParameters(new HashMap<String, String>());
    tbl1.getParameters().put(NUM_FILES, "0");
    tbl1.getParameters().put(TOTAL_SIZE, "0");

    when(wh.getFileStatusesForUnpartitionedTable(db, tbl1)).thenReturn(fileStatus);
    updateTableStatsSlow(db, tbl1, wh, false, true);
    assertThat(tbl1.getParameters(), is(expected));

    // Verify that COLUMN_STATS_ACCURATE is removed from params
    Table tbl2 = new Table();
    tbl2.setDbName(DB_NAME);
    tbl2.setTableName(TABLE_NAME);
    tbl2.setSd(sd);
    tbl2.setParameters(new HashMap<String, String>());
    tbl2.getParameters().put(COLUMN_STATS_ACCURATE, "true");

    when(wh.getFileStatusesForUnpartitionedTable(db, tbl2)).thenReturn(fileStatus);
    updateTableStatsSlow(db, tbl2, wh, false, true);
    assertThat(tbl2.getParameters(), is(expected));
  }

  /**
   * Verify that the call to updateTableStatsSlow() removes DO_NOT_UPDATE_STATS from table params.
   */
  @Test
  public void testUpdateTableStatsSlow_removesDoNotUpdateStats() throws TException {
    // Create database and table
    Table tbl = new Table();
    tbl.setDbName(DB_NAME);
    tbl.setTableName(TABLE_NAME);
    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(Collections.singletonList(new FieldSchema("id", "int", "")));
    sd.setLocation("/tmp");
    tbl.setSd(sd);
    tbl.setParameters(new HashMap<String, String>());
    tbl.getParameters().put(StatsSetupConst.DO_NOT_UPDATE_STATS, "true");

    Table tbl1 = new Table();
    tbl1.setDbName(DB_NAME);
    tbl1.setTableName(TABLE_NAME);
    tbl1.setSd(sd);
    tbl1.setParameters(new HashMap<String, String>());
    tbl1.getParameters().put(StatsSetupConst.DO_NOT_UPDATE_STATS, "false");

    Warehouse wh = mock(Warehouse.class);
    updateTableStatsSlow(db, tbl, wh, false, true);
    Map<String, String> expected = Collections.emptyMap();
    assertThat(tbl.getParameters(), is(expected));
    verify(wh, never()).getFileStatusesForUnpartitionedTable(db, tbl);
    updateTableStatsSlow(db, tbl1, wh, true, false);
    assertThat(tbl.getParameters(), is(expected));
    verify(wh, never()).getFileStatusesForUnpartitionedTable(db, tbl1);
  }

  /**
   * Verify that updateTableStatsSlow() does not calculate table statistics when
   * <ol>
   *   <li>newDir is true</li>
   *   <li>Table is partitioned</li>
   *   <li>Stats are already present and forceRecompute isn't set</li>
   * </ol>
   */
  @Test
  public void testUpdateTableStatsSlow_doesNotUpdateStats() throws TException {
    FieldSchema fs = new FieldSchema("date", "string", "date column");
    List<FieldSchema> cols = Collections.singletonList(fs);

    // Create database and table
    Table tbl = new Table();
    tbl.setDbName(DB_NAME);
    tbl.setTableName(TABLE_NAME);
    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(Collections.singletonList(new FieldSchema("id", "int", "")));
    sd.setLocation("/tmp");
    tbl.setSd(sd);
    tbl.setParameters(new HashMap<String, String>());

    Warehouse wh = mock(Warehouse.class);
    // newDir(true) => stats not updated
    updateTableStatsSlow(db, tbl, wh, true, false);
    verify(wh, never()).getFileStatusesForUnpartitionedTable(db, tbl);

    // partitioned table => stats not updated
    Table tbl1 = new Table();
    tbl1.setDbName(DB_NAME);
    tbl1.setTableName(TABLE_NAME);
    tbl1.setPartitionKeys(cols);
    tbl1.setSd(sd);
    tbl.setParameters(new HashMap<String, String>());

    updateTableStatsSlow(db, tbl1, wh, false, false);
    verify(wh, never()).getFileStatusesForUnpartitionedTable(db, tbl1);

    // Already contains stats => stats not updated when forceRecompute isn't set
    Table tbl2 = new Table();
    tbl2.setDbName(DB_NAME);
    tbl2.setTableName(TABLE_NAME);
    tbl2.setSd(sd);
    tbl2.setParameters(paramsWithStats);

    updateTableStatsSlow(db, tbl2, wh, false, false);
    verify(wh, never()).getFileStatusesForUnpartitionedTable(db, tbl2);
  }

  /**
   * Build a FileStatus object.
   */
  private static FileStatus getFileStatus(long fileLength, boolean isdir, int blockReplication,
                                          int blockSize, int modificationTime, String pathString) {
    return new FileStatus(fileLength, isdir, blockReplication, blockSize, modificationTime, new Path(pathString));
  }

}
