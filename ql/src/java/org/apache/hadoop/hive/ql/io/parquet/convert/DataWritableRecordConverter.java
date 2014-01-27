package org.apache.hadoop.hive.ql.io.parquet.convert;

import org.apache.hadoop.io.ArrayWritable;

import parquet.io.api.GroupConverter;
import parquet.io.api.RecordMaterializer;
import parquet.schema.GroupType;

/**
 *
 * A MapWritableReadSupport, encapsulates the tuples
 *
 */
public class DataWritableRecordConverter extends RecordMaterializer<ArrayWritable> {

  private final DataWritableGroupConverter root;

  public DataWritableRecordConverter(final GroupType requestedSchema, final GroupType tableSchema) {
    this.root = new DataWritableGroupConverter(requestedSchema, tableSchema);
  }

  @Override
  public ArrayWritable getCurrentRecord() {
    return root.getCurrentArray();
  }

  @Override
  public GroupConverter getRootConverter() {
    return root;
  }
}
