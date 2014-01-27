package org.apache.hadoop.hive.ql.io.parquet.convert;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

import parquet.io.ParquetDecodingException;
import parquet.io.api.Converter;
import parquet.schema.GroupType;

/**
 *
 * A ArrayWritableGroupConverter
 *
 */
public class ArrayWritableGroupConverter extends HiveGroupConverter {

  private final Converter[] converters;
  private final HiveGroupConverter parent;
  private final int index;
  private final boolean isMap;
  private Writable currentValue;
  private Writable[] mapPairContainer;

  public ArrayWritableGroupConverter(final GroupType groupType, final HiveGroupConverter parent, final int index) {
    this.parent = parent;
    this.index = index;

    if (groupType.getFieldCount() == 2) {
      converters = new Converter[2];
      converters[0] = getConverterFromDescription(groupType.getType(0), 0, this);
      converters[1] = getConverterFromDescription(groupType.getType(1), 1, this);
      isMap = true;
    } else if (groupType.getFieldCount() == 1) {
      converters = new Converter[1];
      converters[0] = getConverterFromDescription(groupType.getType(0), 0, this);
      isMap = false;
    } else {
      throw new RuntimeException("Invalid parquet hive schema: " + groupType);
    }

  }

  @Override
  public Converter getConverter(final int fieldIndex) {
    return converters[fieldIndex];
  }

  @Override
  public void start() {
    if (isMap) {
      mapPairContainer = new Writable[2];
    }
  }

  @Override
  public void end() {
    if (isMap) {
      currentValue = new ArrayWritable(Writable.class, mapPairContainer);
    }
    parent.add(index, currentValue);
  }

  @Override
  protected void set(final int index, final Writable value) {
    if (index != 0 && mapPairContainer == null || index > 1) {
      throw new ParquetDecodingException("Repeated group can only have one or two fields for maps. Not allowed to set for the index : " + index);
    }

    if (isMap) {
      mapPairContainer[index] = value;
    } else {
      currentValue = value;
    }
  }

  @Override
  protected void add(final int index, final Writable value) {
    set(index, value);
  }
}
