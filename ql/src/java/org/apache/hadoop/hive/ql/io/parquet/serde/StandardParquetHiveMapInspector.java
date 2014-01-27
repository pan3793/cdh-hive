package org.apache.hadoop.hive.ql.io.parquet.serde;

import java.util.Map;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

/**
 * The StandardParquetHiveMapInspector will inspect an ArrayWritable, considering it as a Hive map.<br />
 * It can also inspect a Map if Hive decides to inspect the result of an inspection.
 *
 */
public class StandardParquetHiveMapInspector extends AbstractParquetMapInspector {

  public StandardParquetHiveMapInspector(final ObjectInspector keyInspector, final ObjectInspector valueInspector) {
    super(keyInspector, valueInspector);
  }

  @Override
  public Object getMapValueElement(final Object data, final Object key) {
    if (data == null || key == null) {
      return null;
    }

    if (data instanceof ArrayWritable) {
      final Writable[] mapContainer = ((ArrayWritable) data).get();

      if (mapContainer == null || mapContainer.length == 0) {
        return null;
      }

      final Writable[] mapArray = ((ArrayWritable) mapContainer[0]).get();

      for (final Writable obj : mapArray) {
        final ArrayWritable mapObj = (ArrayWritable) obj;
        final Writable[] arr = mapObj.get();
        if (key.equals(arr[0])) {
          return arr[1];
        }
      }

      return null;
    }

    if (data instanceof Map) {
      return ((Map) data).get(key);
    }

    throw new UnsupportedOperationException("Cannot inspect " + data.getClass().getCanonicalName());
  }
}
