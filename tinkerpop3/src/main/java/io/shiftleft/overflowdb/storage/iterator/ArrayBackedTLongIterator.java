package io.shiftleft.overflowdb.storage.iterator;

import gnu.trove.iterator.TLongIterator;
import org.apache.commons.lang3.NotImplementedException;

/**
 * A TLongIterator backed by an Array.
 * Technically this is nonsense - why would you use an Iterator it holds a element to all the data?
 * Since java arrays don't implement `Iterator`, I didn't find a better way.
 */
public class ArrayBackedTLongIterator implements TLongIterator {

  private final long[] array;
  private int cursor = 0;

  public ArrayBackedTLongIterator(long[] array) {
    this.array = array;
  }

  /* for better performance, use the `long[]` alternative, since it doesn't require boxing/unboxing */
  public ArrayBackedTLongIterator(Long[] array) {
    this.array = new long[array.length];
    for (int i = 0; i < array.length; i++) {
      this.array[i] = array[i];
    }
  }

  @Override
  public boolean hasNext() {
    return array.length > cursor;
  }

  @Override
  public long next() {
    return array[cursor++];
  }

  @Override
  public void remove() {
    throw new NotImplementedException("");
  }

}
