package org.apache.hadoop.hive.ql.io.parquet.write;

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;

import parquet.hadoop.api.WriteSupport;
import parquet.io.api.RecordConsumer;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

/**
 *
 * DataWritableWriteSupport is a WriteSupport for the DataWritableWriter
 *
 */
public class DataWritableWriteSupport extends WriteSupport<ArrayWritable> {

  public static final String PARQUET_HIVE_SCHEMA = "parquet.hive.schema";

  public static void setSchema(final MessageType schema, final Configuration configuration) {
    configuration.set(PARQUET_HIVE_SCHEMA, schema.toString());
  }

  public static MessageType getSchema(final Configuration configuration) {
    return MessageTypeParser.parseMessageType(configuration.get(PARQUET_HIVE_SCHEMA));
  }
  private DataWritableWriter writer;
  private MessageType schema;

  @Override
  public WriteContext init(final Configuration configuration) {
    schema = getSchema(configuration);
    return new WriteContext(schema, new HashMap<String, String>());
  }

  @Override
  public void prepareForWrite(final RecordConsumer recordConsumer) {
    writer = new DataWritableWriter(recordConsumer, schema);
  }

  @Override
  public void write(final ArrayWritable record) {
    writer.write(record);
  }
}
