package org.apache.hadoop.hive.ql.io.parquet.serde.primitive;

/**
 * The ParquetPrimitiveInspectorFactory allows us to be sure that the same object is inspected by the same inspector.
 *
 */
public class ParquetPrimitiveInspectorFactory {

  public static final ParquetByteInspector parquetByteInspector = new ParquetByteInspector();
  public static final ParquetShortInspector parquetShortInspector = new ParquetShortInspector();
  public static final ParquetStringInspector parquetStringInspector = new ParquetStringInspector();

  private ParquetPrimitiveInspectorFactory() {
    // prevent instantiation
  }
}
