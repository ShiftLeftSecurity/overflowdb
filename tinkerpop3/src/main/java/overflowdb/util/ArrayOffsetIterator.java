package overflowdb.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class ArrayOffsetIterator<T> implements Iterator<T> {
  private final Object[] array;
  private int current;
  private final int exclusiveEnd;
  private final int strideSize;

  /** used for peeking */
  private T nextCached;

  public ArrayOffsetIterator(Object[] array, int begin, int exclusiveEnd, int strideSize) {
    this.array = array;
    this.current = begin;
    this.exclusiveEnd = exclusiveEnd;
    this.strideSize = strideSize;
  }

  @Override
  public final boolean hasNext() {
    return peekNext() != null;
  }

  /**
   * Elements within the arrays may be null, e.g. because an edge was deleted.
   * Therefor we need to have the ability to peek forward to the next element.
  */
  private T peekNext() {
    if (nextCached != null)
      return nextCached;

    if (current < exclusiveEnd) {
      nextCached = (T) array[current];
      // n.b. nextCached may be `null`, e.g. if an edge was deleted
      current += strideSize;
      return peekNext();
    }

    return null; // i.e. we have reached the end of this Iterator
  }

  @Override
  public final T next() {
    if (hasNext()) {
      final T ret = peekNext();
      nextCached = null;
      return ret;
    } else {
      throw new NoSuchElementException();
    }
  }
}
