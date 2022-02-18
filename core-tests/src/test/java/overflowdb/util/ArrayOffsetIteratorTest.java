package overflowdb.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ArrayOffsetIteratorTest {

  @Test
  public void empty() {
    ArrayOffsetIterator iter = new ArrayOffsetIterator(new String[]{}, 0, 0, 1);
    assertFalse(iter.hasNext());
  }

  @Test
  public void simple() {
    Object[] array = new String[] { "one", "two" };
    ArrayOffsetIterator iter = new ArrayOffsetIterator(array, 0, 2, 1);
    assertTrue(iter.hasNext());
    assertEquals("one", iter.next());
    assertTrue(iter.hasNext());
    assertEquals("two", iter.next());
    assertFalse(iter.hasNext());
  }

  @Test
  public void hasNextIsIdempotent() {
    Object[] array = new String[] { "one", "two" };
    ArrayOffsetIterator iter = new ArrayOffsetIterator(array, 0, 2, 1);
    assertTrue(iter.hasNext());
    assertTrue(iter.hasNext());
    assertEquals("one", iter.next());
    assertTrue(iter.hasNext());
    assertTrue(iter.hasNext());
    assertEquals("two", iter.next());
    assertFalse(iter.hasNext());
  }

  @Test
  public void part() {
    Object[] array = new String[] { "beforeRelevantPart", "one", "two", "afterRelevantPart" };
    ArrayOffsetIterator iter = new ArrayOffsetIterator(array, 1, 3, 1);
    assertTrue(iter.hasNext());
    assertEquals("one", iter.next());
    assertTrue(iter.hasNext());
    assertEquals("two", iter.next());
    assertFalse(iter.hasNext());
  }

  @Test
  public void strideSize2() {
    Object[] array = new String[] { "one", "other stuff", "two", "other stuff" };
    ArrayOffsetIterator iter = new ArrayOffsetIterator(array, 0, 4, 2);
    assertTrue(iter.hasNext());
    assertEquals("one", iter.next());
    assertTrue(iter.hasNext());
    assertEquals("two", iter.next());
    assertFalse(iter.hasNext());
  }

  @Test
  public void nullEntries() {
    Object[] array = new String[] { "one", null, "two" };
    ArrayOffsetIterator iter = new ArrayOffsetIterator(array, 0, 3, 1);
    assertTrue(iter.hasNext());
    assertEquals("one", iter.next());
    assertTrue(iter.hasNext());
    assertEquals("two", iter.next());
    assertFalse(iter.hasNext());
  }
}
