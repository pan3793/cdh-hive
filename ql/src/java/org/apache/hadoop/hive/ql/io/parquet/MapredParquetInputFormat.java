package org.apache.hadoop.hive.ql.io.parquet;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport;
import org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.RecordReader;

import parquet.hadoop.ParquetInputFormat;


/**
 *
 * A Parquet InputFormat for Hive (with the deprecated package mapred)
 *
 */
public class MapredParquetInputFormat extends FileInputFormat<Void, ArrayWritable> {

  public static final Log LOG = LogFactory.getLog(MapredParquetInputFormat.class);

  private final ParquetInputFormat<ArrayWritable> realInput;

  public MapredParquetInputFormat() {
    this(new ParquetInputFormat<ArrayWritable>(DataWritableReadSupport.class));
  }

  protected MapredParquetInputFormat(final ParquetInputFormat<ArrayWritable> inputFormat) {
    this.realInput = inputFormat;
  }

  @Override
  public org.apache.hadoop.mapred.RecordReader<Void, ArrayWritable> getRecordReader(
      final org.apache.hadoop.mapred.InputSplit split,
      final org.apache.hadoop.mapred.JobConf job,
      final org.apache.hadoop.mapred.Reporter reporter
      ) throws IOException {
    try {
      return (RecordReader<Void, ArrayWritable>) new ParquetRecordReaderWrapper(realInput, split, job, reporter);
    } catch (final InterruptedException e) {
      throw new RuntimeException("Cannot create a RecordReaderWrapper", e);
    }
  }
}
