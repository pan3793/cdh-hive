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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.PartitionSpecWithSharedSD;
import org.apache.hadoop.hive.metastore.api.PartitionWithoutSD;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.thrift.TException;
import org.hamcrest.core.IsNot;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.regex.Pattern.compile;
import static org.apache.hadoop.hive.common.StatsSetupConst.COLUMN_STATS_ACCURATE;
import static org.apache.hadoop.hive.common.StatsSetupConst.NUM_ERASURE_CODED_FILES;
import static org.apache.hadoop.hive.common.StatsSetupConst.NUM_FILES;
import static org.apache.hadoop.hive.common.StatsSetupConst.STATS_GENERATED;
import static org.apache.hadoop.hive.common.StatsSetupConst.TOTAL_SIZE;
import static org.apache.hadoop.hive.metastore.MetaStoreUtils.filterMapkeys;
import static org.apache.hadoop.hive.metastore.MetaStoreUtils.updateTableStatsSlow;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
          TOTAL_SIZE, "2",
          NUM_ERASURE_CODED_FILES, "0"
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
   *   <li>Verifies behavior when STATS_GENERATED is set in environment context</li>
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
    FileStatus fs1 = getFileStatus(1, true, 2, 3, 4, "/tmp/0", false);
    FileStatus fs2 = getFileStatus(fileLength, false, 3, 4, 5, "/tmp/1", true);
    FileStatus fs3 = getFileStatus(fileLength, false, 3, 4, 5, "/tmp/1", false);
    FileStatus[] fileStatus = {fs1, fs2, fs3};
    Warehouse wh = mock(Warehouse.class);
    when(wh.getFileStatusesForUnpartitionedTable(db, tbl)).thenReturn(fileStatus);

    Map<String, String> expected = ImmutableMap.of(NUM_FILES, "2",
            TOTAL_SIZE, String.valueOf(2 * fileLength),
            NUM_ERASURE_CODED_FILES, "1"
    );
    updateTableStatsSlow(db, tbl, wh, false, false, null);
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
    updateTableStatsSlow(db, tbl1, wh, false, true, null);
    assertThat(tbl1.getParameters(), is(expected));

    // Verify that COLUMN_STATS_ACCURATE is removed from params
    Table tbl2 = new Table();
    tbl2.setDbName(DB_NAME);
    tbl2.setTableName(TABLE_NAME);
    tbl2.setSd(sd);
    tbl2.setParameters(new HashMap<String, String>());
    tbl2.getParameters().put(COLUMN_STATS_ACCURATE, "true");

    when(wh.getFileStatusesForUnpartitionedTable(db, tbl2)).thenReturn(fileStatus);
    updateTableStatsSlow(db, tbl2, wh, false, true, null);
    assertThat(tbl2.getParameters(), is(expected));

    EnvironmentContext context = new EnvironmentContext(ImmutableMap.of(STATS_GENERATED,
        StatsSetupConst.TASK));

    // Verify that if environment context has STATS_GENERATED set to task,
    // COLUMN_STATS_ACCURATE in params is set to correct value
    Table tbl3 = new Table();
    tbl3.setDbName(DB_NAME);
    tbl3.setTableName(TABLE_NAME);
    tbl3.setSd(sd);
    tbl3.setParameters(new HashMap<String, String>());
    tbl3.getParameters().put(COLUMN_STATS_ACCURATE, "foo");
    when(wh.getFileStatusesForUnpartitionedTable(db, tbl3)).thenReturn(fileStatus);
    updateTableStatsSlow(db, tbl3, wh, false, true, context);

    Map<String, String> expected1 = ImmutableMap.of(NUM_FILES, "2",
        TOTAL_SIZE, String.valueOf(2 * fileLength),
        NUM_ERASURE_CODED_FILES, "1",
        COLUMN_STATS_ACCURATE, "{\"BASIC_STATS\":\"true\"}");
    assertThat(tbl3.getParameters(), is(expected1));
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
    updateTableStatsSlow(db, tbl, wh, false, true, null);
    Map<String, String> expected = Collections.emptyMap();
    assertThat(tbl.getParameters(), is(expected));
    verify(wh, never()).getFileStatusesForUnpartitionedTable(db, tbl);
    updateTableStatsSlow(db, tbl1, wh, true, false, null);
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
    updateTableStatsSlow(db, tbl, wh, true, false, null);
    verify(wh, never()).getFileStatusesForUnpartitionedTable(db, tbl);

    // partitioned table => stats not updated
    Table tbl1 = new Table();
    tbl1.setDbName(DB_NAME);
    tbl1.setTableName(TABLE_NAME);
    tbl1.setPartitionKeys(cols);
    tbl1.setSd(sd);
    tbl.setParameters(new HashMap<String, String>());

    updateTableStatsSlow(db, tbl1, wh, false, false, null);
    verify(wh, never()).getFileStatusesForUnpartitionedTable(db, tbl1);

    // Already contains stats => stats not updated when forceRecompute isn't set
    Table tbl2 = new Table();
    tbl2.setDbName(DB_NAME);
    tbl2.setTableName(TABLE_NAME);
    tbl2.setSd(sd);
    tbl2.setParameters(paramsWithStats);

    updateTableStatsSlow(db, tbl2, wh, false, false, null);
    verify(wh, never()).getFileStatusesForUnpartitionedTable(db, tbl2);
  }

  /**
   * Build a FileStatus object.
   */
  private static FileStatus getFileStatus(long fileLength, boolean isdir, int blockReplication,
          int blockSize, int modificationTime, String pathString, boolean isErasureCoded) {
    return new FileStatus(fileLength, isdir, blockReplication, blockSize, modificationTime,
            +0L, (FsPermission) null, (String) null, (String) null, null,
             new Path(pathString), false, false, isErasureCoded);
  }

  @Test
  public void testFilterMapWithPredicates() {
    Map<String, String> testMap = getTestParamMap();

    List<String> excludePatterns = Arrays.asList("lastDdl", "num");
    testMapFilter(testMap, excludePatterns);
    assertFalse(testMap.containsKey("transient_lastDdlTime"));
    assertFalse(testMap.containsKey("numFiles"));
    assertFalse(testMap.containsKey("numFilesErasureCoded"));
    assertFalse(testMap.containsKey("numRows"));

    Map<String, String> expectedMap = new HashMap<String, String>() {{
        put("totalSize", "1024");
        put("rawDataSize", "3243234");
        put("COLUMN_STATS_ACCURATE", "{\"BASIC_STATS\":\"true\"");
        put("COLUMN_STATS_ACCURATED", "dummy");
        put("bucketing_version", "2");
        put("testBucketing_version", "2");
      }};

    assertThat(expectedMap, is(testMap));

    testMap = getTestParamMap();
    excludePatterns = Arrays.asList("^bucket", "ACCURATE$");
    testMapFilter(testMap, excludePatterns);

    expectedMap = new HashMap<String, String>() {{
        put("totalSize", "1024");
        put("numRows", "10");
        put("rawDataSize", "3243234");
        put("COLUMN_STATS_ACCURATED", "dummy");
        put("numFiles", "2");
        put("transient_lastDdlTime", "1537487124");
        put("testBucketing_version", "2");
        put("numFilesErasureCoded", "0");
      }};

    assertThat(expectedMap, is(testMap));

    // Test for overlapping predicates on same key.
    testMap = getTestParamMap();
    excludePatterns = Arrays.asList("^bucket", "version$");
    testMapFilter(testMap, excludePatterns);

    expectedMap = new HashMap<String, String>() {{
        put("totalSize", "1024");
        put("numRows", "10");
        put("rawDataSize", "3243234");
        put("COLUMN_STATS_ACCURATE", "{\"BASIC_STATS\":\"true\"");
        put("COLUMN_STATS_ACCURATED", "dummy");
        put("numFiles", "2");
        put("transient_lastDdlTime", "1537487124");
        put("numFilesErasureCoded", "0");
      }};

    assertThat(expectedMap, is(testMap));

    // test that if the config is not set in MetastoreConf, it does not filter any parameter
    Configuration testConf = new HiveConf();
    testMap = getTestParamMap();
    excludePatterns = Arrays.asList(HiveConf
        .getTrimmedStringsVar(testConf, HiveConf.ConfVars.EVENT_NOTIFICATION_PARAMETERS_EXCLUDE_PATTERNS));

    testMapFilter(testMap, excludePatterns);
    assertThat(getTestParamMap(), is(testMap));


    // test that if the config is set to empty String in MetastoreConf, it does not filter any parameter
    testConf.setStrings(HiveConf.ConfVars.EVENT_NOTIFICATION_PARAMETERS_EXCLUDE_PATTERNS.varname, "");
    testMap = getTestParamMap();
    excludePatterns = Arrays.asList(HiveConf
        .getTrimmedStringsVar(testConf, HiveConf.ConfVars.EVENT_NOTIFICATION_PARAMETERS_EXCLUDE_PATTERNS));

    testMapFilter(testMap, excludePatterns);
    assertThat(getTestParamMap(), is(testMap));
  }

  private void testMapFilter(Map<String, String> testMap, List<String> patterns) {
    List<Predicate<String>> paramsFilter =
        patterns.stream().map(pattern -> compile(pattern).asPredicate()).collect(Collectors.toList());
    filterMapkeys(testMap, paramsFilter);
  }

  private Map<String, String> getTestParamMap() {
    return new HashMap<String, String>() {{
        put("totalSize", "1024");
        put("numRows", "10");
        put("rawDataSize", "3243234");
        put("COLUMN_STATS_ACCURATE", "{\"BASIC_STATS\":\"true\"");
        put("COLUMN_STATS_ACCURATED", "dummy");
        put("numFiles", "2");
        put("transient_lastDdlTime", "1537487124");
        put("bucketing_version", "2");
        put("testBucketing_version", "2");
        put("numFilesErasureCoded", "0");
      }};
  }
  /**
   * Two empty StorageDescriptorKey should be equal.
   */
  @Test
  public void testCompareNullSdKey() {
    assertThat(MetaStoreUtils.StorageDescriptorKey.UNSET_KEY,
        is(new MetaStoreUtils.StorageDescriptorKey()));
  }

  /**
   * Two StorageDescriptorKey objects with null storage descriptors should be
   * equal iff the base location is equal.
   */
  @Test
  public void testCompareNullSd()
  {
    assertThat(new MetaStoreUtils.StorageDescriptorKey("a", null),
        is(new MetaStoreUtils.StorageDescriptorKey("a", null)));
    // Different locations produce different objects
    assertThat(new MetaStoreUtils.StorageDescriptorKey("a", null),
        IsNot.not(equalTo(new MetaStoreUtils.StorageDescriptorKey("b", null))));
  }

  /**
   * Two StorageDescriptorKey objects with the same base location but different
   * SD location should be equal
   */
  @Test
  public void testCompareWithSdSamePrefixDifferentLocation() throws MetaException {
    Partition p1 = new PartitionBuilder()
        .setDbName("DB_NAME")
        .setTableName(TABLE_NAME)
        .setLocation("l1")
        .addCol("a", "int")
        .addValue("val1")
        .build();
    Partition p2 = new PartitionBuilder()
        .setDbName("DB_NAME")
        .setTableName(TABLE_NAME)
        .setLocation("l2")
        .addCol("a", "int")
        .addValue("val1")
        .build();
    assertThat(new MetaStoreUtils.StorageDescriptorKey("a", p1.getSd()),
        is(new MetaStoreUtils.StorageDescriptorKey("a", p2.getSd())));
  }

  /**
   * Two StorageDescriptorKey objects with the same base location
   * should be equal iff their columns are equal
   */
  @Test
  public void testCompareWithSdSamePrefixDifferentCols() throws MetaException {
    Partition p1 = new PartitionBuilder()
        .setDbName("DB_NAME")
        .setTableName(TABLE_NAME)
        .setLocation("l1")
        .addCol("a", "int")
        .addValue("val1")
        .build();
    Partition p2 = new PartitionBuilder()
        .setDbName("DB_NAME")
        .setTableName(TABLE_NAME)
        .setLocation("l2")
        .addCol("b", "int")
        .addValue("val1")
        .build();
    Partition p3 = new PartitionBuilder()
        .setDbName("DB_NAME")
        .setTableName(TABLE_NAME)
        .setLocation("l2")
        .addCol("a", "int")
        .addValue("val1")
        .build();
    assertThat(new MetaStoreUtils.StorageDescriptorKey("a", p1.getSd()),
        IsNot.not(new MetaStoreUtils.StorageDescriptorKey("a", p2.getSd())));
    assertThat(new MetaStoreUtils.StorageDescriptorKey("a", p1.getSd()),
        is(new MetaStoreUtils.StorageDescriptorKey("a", p3.getSd())));
  }

  /**
   * Two StorageDescriptorKey objects with the same base location
   * should be equal iff their output formats are equal
   */
  @Test
  public void testCompareWithSdSamePrefixDifferentOutputFormat() throws MetaException {
    Partition p1 = new PartitionBuilder()
        .setDbName("DB_NAME")
        .setTableName(TABLE_NAME)
        .setLocation("l1")
        .addCol("a", "int")
        .addValue("val1")
        .setOutputFormat("foo")
        .build();
    Partition p2 = new PartitionBuilder()
        .setDbName("DB_NAME")
        .setTableName(TABLE_NAME)
        .setLocation("l2")
        .addCol("a", "int")
        .setOutputFormat("bar")
        .addValue("val1")
        .build();
    Partition p3 = new PartitionBuilder()
        .setDbName("DB_NAME")
        .setTableName(TABLE_NAME)
        .setLocation("l2")
        .addCol("a", "int")
        .setOutputFormat("foo")
        .addValue("val1")
        .build();
    assertThat(new MetaStoreUtils.StorageDescriptorKey("a", p1.getSd()),
        IsNot.not(new MetaStoreUtils.StorageDescriptorKey("a", p2.getSd())));
    assertThat(new MetaStoreUtils.StorageDescriptorKey("a", p1.getSd()),
        is(new MetaStoreUtils.StorageDescriptorKey("a", p3.getSd())));
  }

  /**
   * Two StorageDescriptorKey objects with the same base location
   * should be equal iff their input formats are equal
   */
  @Test
  public void testCompareWithSdSamePrefixDifferentInputFormat() throws MetaException {
    Partition p1 = new PartitionBuilder()
        .setDbName("DB_NAME")
        .setTableName(TABLE_NAME)
        .setLocation("l1")
        .addCol("a", "int")
        .addValue("val1")
        .setInputFormat("foo")
        .build();
    Partition p2 = new PartitionBuilder()
        .setDbName("DB_NAME")
        .setTableName(TABLE_NAME)
        .setLocation("l2")
        .addCol("a", "int")
        .setInputFormat("bar")
        .addValue("val1")
        .build();
    Partition p3 = new PartitionBuilder()
        .setDbName("DB_NAME")
        .setTableName(TABLE_NAME)
        .setLocation("l1")
        .addCol("a", "int")
        .addValue("val1")
        .setInputFormat("foo")
        .build();
    assertThat(new MetaStoreUtils.StorageDescriptorKey("a", p1.getSd()),
        IsNot.not(new MetaStoreUtils.StorageDescriptorKey("a", p2.getSd())));
    assertThat(new MetaStoreUtils.StorageDescriptorKey("a", p1.getSd()),
        is(new MetaStoreUtils.StorageDescriptorKey("a", p3.getSd())));
  }

  /**
   * Test getPartitionspecsGroupedByStorageDescriptor() for partitions with null SDs.
   */
  @Test
  public void testGetPartitionspecsGroupedBySDNullSD() throws MetaException {
    // Create database and table
    Table tbl = new TableBuilder()
        .setDbName(DB_NAME)
        .setTableName(TABLE_NAME)
        .addCol("id", "int")
        .setLocation("/foo")
        .build();
    Partition p1 = new PartitionBuilder()
        .setDbName("DB_NAME")
        .setTableName(TABLE_NAME)
        .addCol("a", "int")
        .addValue("val1")
        .setInputFormat("foo")
        .build();
    // Set SD to null
    p1.unsetSd();
    assertThat(p1.getSd(), is((StorageDescriptor)null));
    List<PartitionSpec> result =
        MetaStoreUtils.getPartitionspecsGroupedByStorageDescriptor(tbl, Collections.singleton(p1));
    assertThat(result.size(), is(1));
    PartitionSpec ps = result.get(0);
    assertThat(ps.getRootPath(), is((String)null));
    List<PartitionWithoutSD> partitions = ps.getSharedSDPartitionSpec().getPartitions();
    assertThat(partitions.size(), is(1));
    PartitionWithoutSD partition = partitions.get(0);
    assertThat(partition.getRelativePath(), is((String)null));
    assertThat(partition.getValues(), is(Collections.singletonList("val1")));
  }

  /**
   * Test getPartitionspecsGroupedByStorageDescriptor() for partitions with a single
   * partition which is located under table location.
   */
  @Test
  public void testGetPartitionspecsGroupedBySDOnePartitionInTable() throws MetaException {
    // Create database and table
    Table tbl = new TableBuilder()
        .setDbName(DB_NAME)
        .setTableName(TABLE_NAME)
        .addCol("id", "int")
        .setLocation("/foo")
        .build();
    Partition p1 = new PartitionBuilder()
        .setDbName("DB_NAME")
        .setTableName(TABLE_NAME)
        .setLocation("/foo/bar")
        .addCol("a", "int")
        .addValue("val1")
        .setInputFormat("foo")
        .build();
    List<PartitionSpec> result =
        MetaStoreUtils.getPartitionspecsGroupedByStorageDescriptor(tbl, Collections.singleton(p1));
    assertThat(result.size(), is(1));
    PartitionSpec ps = result.get(0);
    assertThat(ps.getRootPath(), is(tbl.getSd().getLocation()));
    List<PartitionWithoutSD> partitions = ps.getSharedSDPartitionSpec().getPartitions();
    assertThat(partitions.size(), is(1));
    PartitionWithoutSD partition = partitions.get(0);
    assertThat(partition.getRelativePath(), is("/bar"));
    assertThat(partition.getValues(), is(Collections.singletonList("val1")));
  }

  /**
   * Test getPartitionspecsGroupedByStorageDescriptor() for partitions with a single
   * partition which is located outside table location.
   */
  @Test
  public void testGetPartitionspecsGroupedBySDonePartitionExternal() throws MetaException {
    // Create database and table
    Table tbl = new TableBuilder()
        .setDbName(DB_NAME)
        .setTableName(TABLE_NAME)
        .addCol("id", "int")
        .setLocation("/foo")
        .build();
    Partition p1 = new PartitionBuilder()
        .setDbName("DB_NAME")
        .setTableName(TABLE_NAME)
        .setLocation("/a/b")
        .addCol("a", "int")
        .addValue("val1")
        .setInputFormat("foo")
        .build();
    List<PartitionSpec> result =
        MetaStoreUtils.getPartitionspecsGroupedByStorageDescriptor(tbl, Collections.singleton(p1));
    assertThat(result.size(), is(1));
    PartitionSpec ps = result.get(0);
    assertThat(ps.getRootPath(), is((String)null));
    List<Partition>partitions = ps.getPartitionList().getPartitions();
    assertThat(partitions.size(), is(1));
    Partition partition = partitions.get(0);
    assertThat(partition.getSd().getLocation(), is("/a/b"));
    assertThat(partition.getValues(), is(Collections.singletonList("val1")));
  }

  /**
   * Test getPartitionspecsGroupedByStorageDescriptor() multiple partitions:
   * <ul>
   *   <li>Partition with null SD</li>
   *   <li>Two partitions under the table location</li>
   *   <li>One partition outside of table location</li>
   * </ul>
   */
  @Test
  public void testGetPartitionspecsGroupedBySDonePartitionCombined() throws MetaException {
    // Create database and table
    String sharedInputFormat = "foo1";

    Table tbl = new TableBuilder()
        .setDbName(DB_NAME)
        .setTableName(TABLE_NAME)
        .addCol("id", "int")
        .setLocation("/foo")
        .build();
    Partition p1 = new PartitionBuilder()
        .setDbName("DB_NAME")
        .setTableName(TABLE_NAME)
        .setLocation("/foo/bar")
        .addCol("a1", "int")
        .addValue("val1")
        .setInputFormat(sharedInputFormat)
        .build();
    Partition p2 = new PartitionBuilder()
        .setDbName("DB_NAME")
        .setTableName(TABLE_NAME)
        .setLocation("/a/b")
        .addCol("a2", "int")
        .addValue("val2")
        .setInputFormat("foo2")
        .build();
    Partition p3 = new PartitionBuilder()
        .setDbName("DB_NAME")
        .setTableName(TABLE_NAME)
        .addCol("a3", "int")
        .addValue("val3")
        .setInputFormat("foo3")
        .build();
    Partition p4 = new PartitionBuilder()
        .setDbName("DB_NAME")
        .setTableName("TABLE_NAME")
        .setLocation("/foo/baz")
        .addCol("a1", "int")
        .addValue("val4")
        .setInputFormat(sharedInputFormat)
        .build();
    p3.unsetSd();
    List<PartitionSpec> result =
        MetaStoreUtils.getPartitionspecsGroupedByStorageDescriptor(tbl,
            Arrays.asList(p1, p2, p3, p4));
    assertThat(result.size(), is(3));
    PartitionSpec ps1 = result.get(0);
    assertThat(ps1.getRootPath(), is((String)null));
    assertThat(ps1.getPartitionList(), is((List<Partition>)null));
    PartitionSpecWithSharedSD partSpec = ps1.getSharedSDPartitionSpec();
    List<PartitionWithoutSD> partitions1 = partSpec.getPartitions();
    assertThat(partitions1.size(), is(1));
    PartitionWithoutSD partition1 = partitions1.get(0);
    assertThat(partition1.getRelativePath(), is((String)null));
    assertThat(partition1.getValues(), is(Collections.singletonList("val3")));

    PartitionSpec ps2 = result.get(1);
    assertThat(ps2.getRootPath(), is(tbl.getSd().getLocation()));
    assertThat(ps2.getPartitionList(), is((List<Partition>)null));
    List<PartitionWithoutSD> partitions2 = ps2.getSharedSDPartitionSpec().getPartitions();
    assertThat(partitions2.size(), is(2));
    PartitionWithoutSD partition2_1 = partitions2.get(0);
    PartitionWithoutSD partition2_2 = partitions2.get(1);
    if (partition2_1.getRelativePath().equals("baz")) {
      // Swap p2_1 and p2_2
      PartitionWithoutSD tmp = partition2_1;
      partition2_1 = partition2_2;
      partition2_2 = tmp;
    }
    assertThat(partition2_1.getRelativePath(), is("/bar"));
    assertThat(partition2_1.getValues(), is(Collections.singletonList("val1")));
    assertThat(partition2_2.getRelativePath(), is("/baz"));
    assertThat(partition2_2.getValues(), is(Collections.singletonList("val4")));

    PartitionSpec ps4 = result.get(2);
    assertThat(ps4.getRootPath(), is((String)null));
    assertThat(ps4.getSharedSDPartitionSpec(), is((PartitionSpecWithSharedSD)null));
    List<Partition>partitions = ps4.getPartitionList().getPartitions();
    assertThat(partitions.size(), is(1));
    Partition partition = partitions.get(0);
    assertThat(partition.getSd().getLocation(), is("/a/b"));
    assertThat(partition.getValues(), is(Collections.singletonList("val2")));
  }

  @Test
  public void testAnonymizeConnectionURL() {
    String connectionURL = null;
    String expectedConnectionURL = null;
    String result = MetaStoreUtils.anonymizeConnectionURL(connectionURL);
    assertEquals(expectedConnectionURL, result);

    connectionURL = "jdbc:mysql://localhost:1111/db?user=user&password=password";
    expectedConnectionURL = "jdbc:mysql://localhost:1111/db?user=****&password=****";
    result = MetaStoreUtils.anonymizeConnectionURL(connectionURL);
    assertEquals(expectedConnectionURL, result);

    connectionURL = "jdbc:derby:sample;user=jill;password=toFetchAPail";
    expectedConnectionURL = "jdbc:derby:sample;user=****;password=****";
    result = MetaStoreUtils.anonymizeConnectionURL(connectionURL);
    assertEquals(expectedConnectionURL, result);

    connectionURL = "jdbc:mysql://[(host=myhost1,port=1111,user=sandy,password=secret)," +
            "(host=myhost2,port=2222,user=finn,password=secret)]/db";
    expectedConnectionURL = "jdbc:mysql://[(host=myhost1,port=1111,user=****,password=****)," +
            "(host=myhost2,port=2222,user=****,password=****)]/db";
    result = MetaStoreUtils.anonymizeConnectionURL(connectionURL);
    assertEquals(expectedConnectionURL, result);

    connectionURL = "jdbc:derby:memory:${test.tmp.dir}/junit_metastore_db;create=true";
    result = MetaStoreUtils.anonymizeConnectionURL(connectionURL);
    assertEquals(connectionURL, result);
  }

}
