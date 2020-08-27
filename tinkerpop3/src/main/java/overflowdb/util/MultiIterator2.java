package overflowdb.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Subclass-safe variant of MultiIterator from Tinkerpop
 * // TODO drop 2 suffix once tinkerpop is gone
 */
public final class MultiIterator2<T> implements Iterator<T>, Serializable {

  public static <A> MultiIterator2<A> from(Iterator<A>... iterators) {
    MultiIterator2<A> result = new MultiIterator2<>();
    for (Iterator iter : iterators) result.addIterator(iter);
    return result;
  }

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
    if (this.iterators.isEmpty()) throw new NoSuchElementException();

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
    throw new NoSuchElementException();
  }

  public void clear() {
    this.iterators.clear();
    this.current = 0;
  }

}
