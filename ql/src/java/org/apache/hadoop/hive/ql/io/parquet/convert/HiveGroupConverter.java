package org.apache.hadoop.hive.ql.io.parquet.convert;

import org.apache.hadoop.io.Writable;

import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.schema.Type;
import parquet.schema.Type.Repetition;

public abstract class HiveGroupConverter extends GroupConverter {

  static protected Converter getConverterFromDescription(final Type type, final int index, final HiveGroupConverter parent) {
    if (type == null) {
      return null;
    }

    if (type.isPrimitive()) {
      return ETypeConverter.getNewConverter(type.asPrimitiveType().getPrimitiveTypeName().javaType, index, parent);
    } else {
      if (type.asGroupType().getRepetition() == Repetition.REPEATED) {
        return new ArrayWritableGroupConverter(type.asGroupType(), parent, index);
      } else {
        return new DataWritableGroupConverter(type.asGroupType(), parent, index);
      }
    }
  }

  abstract protected void set(int index, Writable value);

  abstract protected void add(int index, Writable value);

}
