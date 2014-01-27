package org.apache.hadoop.hive.ql.io.parquet.serde.primitive;

import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableByteObjectInspector;
import org.apache.hadoop.io.IntWritable;

/**
 * The ParquetByteInspector can inspect both ByteWritables and IntWritables into bytes.
 *
 */
public class ParquetByteInspector extends AbstractPrimitiveJavaObjectInspector implements SettableByteObjectInspector {

  ParquetByteInspector() {
    super(TypeInfoFactory.byteTypeInfo);
  }

  @Override
  public Object getPrimitiveWritableObject(final Object o) {
    return o == null ? null : new ByteWritable(get(o));
  }

  @Override
  public Object create(final byte val) {
    return new ByteWritable(val);
  }

  @Override
  public Object set(final Object o, final byte val) {
    ((ByteWritable) o).set(val);
    return o;
  }

  @Override
  public byte get(Object o) {
    // Accept int writables and convert them.
    if (o instanceof IntWritable) {
      return (byte) ((IntWritable) o).get();
    }
    return ((ByteWritable) o).get();
  }
}
