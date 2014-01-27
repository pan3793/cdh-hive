package org.apache.hadoop.hive.ql.io.parquet.serde.primitive;

import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableShortObjectInspector;
import org.apache.hadoop.io.IntWritable;

/**
 * The ParquetShortInspector can inspect both ShortWritables and IntWritables into shorts.
 *
 */
public class ParquetShortInspector extends AbstractPrimitiveJavaObjectInspector implements SettableShortObjectInspector {

  ParquetShortInspector() {
    super(TypeInfoFactory.shortTypeInfo);
  }

  @Override
  public Object getPrimitiveWritableObject(final Object o) {
    return o == null ? null : new ShortWritable(get(o));
  }

  @Override
  public Object create(final short val) {
    return new ShortWritable(val);
  }

  @Override
  public Object set(final Object o, final short val) {
    ((ShortWritable) o).set(val);
    return o;
  }

  @Override
  public short get(Object o) {
    // Accept int writables and convert them.
    if (o instanceof IntWritable) {
      return (short) ((IntWritable) o).get();
    }
    return ((ShortWritable) o).get();
  }
}
