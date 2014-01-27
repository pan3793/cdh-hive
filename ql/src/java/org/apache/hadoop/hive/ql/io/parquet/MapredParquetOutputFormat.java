package org.apache.hadoop.hive.ql.io.parquet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.convert.HiveSchemaConverter;
import org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriteSupport;
import org.apache.hadoop.hive.ql.io.parquet.write.ParquetRecordWriterWrapper;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.hive.ql.io.FSRecordWriter;

import parquet.hadoop.ParquetOutputFormat;

/**
 *
 * A Parquet OutputFormat for Hive (with the deprecated package mapred)
 *
 */
public class MapredParquetOutputFormat extends FileOutputFormat<Void, ArrayWritable> implements
  HiveOutputFormat<Void, ArrayWritable> {

  public static final Log LOG = LogFactory.getLog(MapredParquetOutputFormat.class);

  protected ParquetOutputFormat<ArrayWritable> realOutputFormat;

  public MapredParquetOutputFormat() {
    realOutputFormat = new ParquetOutputFormat<ArrayWritable>(new DataWritableWriteSupport());
  }

  public MapredParquetOutputFormat(final OutputFormat<Void, ArrayWritable> mapreduceOutputFormat) {
    realOutputFormat = (ParquetOutputFormat<ArrayWritable>) mapreduceOutputFormat;
  }

  @Override
  public void checkOutputSpecs(final FileSystem ignored, final JobConf job) throws IOException {
    realOutputFormat.checkOutputSpecs(ShimLoader.getHadoopShims().getHCatShim().createJobContext(job, null));
  }

  @Override
  public RecordWriter<Void, ArrayWritable> getRecordWriter(
      final FileSystem ignored,
      final JobConf job,
      final String name,
      final Progressable progress
      ) throws IOException {
    throw new RuntimeException("Should never be used");
  }

  /**
   *
   * Create the parquet schema from the hive schema, and return the RecordWriterWrapper which
   * contains the real output format
   */
  @Override
  public FSRecordWriter getHiveRecordWriter(
      final JobConf jobConf,
      final Path finalOutPath,
      final Class<? extends Writable> valueClass,
      final boolean isCompressed,
      final Properties tableProperties,
      final Progressable progress) throws IOException {

    LOG.info("getHiveRecordWriter " + this);
    LOG.info("creating new record writer...");

    // Seriously?  Hard coded property names?
    final String columnNameProperty = tableProperties.getProperty("columns");
    final String columnTypeProperty = tableProperties.getProperty("columns.types");
    List<String> columnNames;
    List<TypeInfo> columnTypes;

    if (columnNameProperty.length() == 0) {
      columnNames = new ArrayList<String>();
    } else {
      columnNames = Arrays.asList(columnNameProperty.split(","));
    }

    if (columnTypeProperty.length() == 0) {
      columnTypes = new ArrayList<TypeInfo>();
    } else {
      columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    }

    DataWritableWriteSupport.setSchema(HiveSchemaConverter.convert(columnNames, columnTypes), jobConf);
    return getParquerRecordWriterWrapper(realOutputFormat, jobConf, finalOutPath.toString(), progress);
  }

  protected ParquetRecordWriterWrapper getParquerRecordWriterWrapper(
      ParquetOutputFormat<ArrayWritable> realOutputFormat,
      JobConf jobConf,
      String finalOutPath,
      Progressable progress
      ) throws IOException {
    return new ParquetRecordWriterWrapper(realOutputFormat, jobConf, finalOutPath.toString(), progress);
  }
}
