package org.apache.hadoop.hive.ql.io.parquet.serde;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

/**
 *
 * @author Rémy Pecqueur <r.pecqueur@criteo.com>
 */
public class TestParquetHiveArrayInspector extends TestCase {

  private ParquetHiveArrayInspector inspector;

  @Override
  public void setUp() {
    inspector = new ParquetHiveArrayInspector(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
  }

  @Test
  public void testNullArray() {
    assertEquals("Wrong size", -1, inspector.getListLength(null));
    assertNull("Should be null", inspector.getList(null));
    assertNull("Should be null", inspector.getListElement(null, 0));
  }

  @Test
  public void testNullContainer() {
    final ArrayWritable list = new ArrayWritable(ArrayWritable.class, null);
    assertEquals("Wrong size", -1, inspector.getListLength(list));
    assertNull("Should be null", inspector.getList(list));
    assertNull("Should be null", inspector.getListElement(list, 0));
  }

  @Test
  public void testEmptyContainer() {
    final ArrayWritable list = new ArrayWritable(ArrayWritable.class, new ArrayWritable[0]);
    assertEquals("Wrong size", -1, inspector.getListLength(list));
    assertNull("Should be null", inspector.getList(list));
    assertNull("Should be null", inspector.getListElement(list, 0));
  }

  @Test
  public void testRegularList() {
    final ArrayWritable internalList = new ArrayWritable(Writable.class,
            new Writable[]{new IntWritable(3), new IntWritable(5), new IntWritable(1)});
    final ArrayWritable list = new ArrayWritable(ArrayWritable.class, new ArrayWritable[]{internalList});

    final List<Writable> expected = new ArrayList<Writable>();
    expected.add(new IntWritable(3));
    expected.add(new IntWritable(5));
    expected.add(new IntWritable(1));

    assertEquals("Wrong size", 3, inspector.getListLength(list));
    assertEquals("Wrong result of inspection", expected, inspector.getList(list));

    for (int i = 0; i < expected.size(); ++i) {
      assertEquals("Wrong result of inspection", expected.get(i), inspector.getListElement(list, i));

    }

    assertNull("Should be null", inspector.getListElement(list, 3));
  }
}
