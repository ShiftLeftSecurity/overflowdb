package io.shiftleft.overflowdb.storage.iterator;

import gnu.trove.iterator.TLongIterator;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;

import java.util.List;

/**
 * Analogous to `MultiIterator`, but specific for TLongIterators
 * This wouldn't be necessary if only java/trove iterators had `flatMap` :(
 */
public class TLongMultiIterator implements TLongIterator {

  private final List<TLongIterator> iterators;
  private int current = 0;

  public TLongMultiIterator(List<TLongIterator> iterators) {
    this.iterators = iterators;
  }

  @Override
  public boolean hasNext() {
    if (this.current >= iterators.size())
      return false;

    TLongIterator currentIterator = iterators.get(this.current);

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
  public long next() {
    if (iterators.isEmpty()) throw FastNoSuchElementException.instance();

    TLongIterator currentIterator = iterators.get(this.current);
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

  @Override
  public void remove() {
    throw new NotImplementedException("");
  }
}
