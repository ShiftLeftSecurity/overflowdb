package io.shiftleft.overflowdb.util;

import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Subclass-safe variant of MultiIterator
 */
public final class MultiIterator2<T> implements Iterator<T>, Serializable {

  private final List<Iterator<? extends T>> iterators = new ArrayList<>();
  private int current = 0;

  public void addIterator(final Iterator<? extends T> iterator) {
    this.iterators.add(iterator);
  }

  @Override
  public boolean hasNext() {
    if (this.current >= this.iterators.size())
      return false;

    Iterator currentIterator = this.iterators.get(this.current);

    while (true) {
      if (currentIterator.hasNext()) {
        return true;
      } else {
        this.current++;
        if (this.current >= iterators.size())
          break;
        currentIterator = iterators.get(this.current);
      }
    }
    return false;
  }

  @Override
  public void remove() {
    this.iterators.get(this.current).remove();
  }

  @Override
  public T next() {
    if (this.iterators.isEmpty()) throw FastNoSuchElementException.instance();

    Iterator<? extends T> currentIterator = iterators.get(this.current);
    while (true) {
      if (currentIterator.hasNext()) {
        return currentIterator.next();
      } else {
        this.current++;
        if (this.current >= iterators.size())
          break;
        currentIterator = iterators.get(current);
      }
    }
    throw FastNoSuchElementException.instance();
  }

  public void clear() {
    this.iterators.clear();
    this.current = 0;
  }

}
