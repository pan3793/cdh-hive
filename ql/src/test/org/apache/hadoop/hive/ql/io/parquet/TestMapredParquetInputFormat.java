package org.apache.hadoop.hive.ql.io.parquet;

import static org.mockito.Mockito.mock;

import org.apache.hadoop.io.ArrayWritable;
import org.junit.Test;

import parquet.hadoop.ParquetInputFormat;

/**
 *
 * Tests for MapredParquetInputFormat.
 *
 * @author Justin Coffey <j.coffey@criteo.com>
 *
 */
public class TestMapredParquetInputFormat {
  @Test
  public void testDefaultConstructor() {
    new MapredParquetInputFormat();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testConstructorWithParquetInputFormat() {
    new MapredParquetInputFormat(
        (ParquetInputFormat<ArrayWritable>) mock(ParquetInputFormat.class)
        );
  }

}
