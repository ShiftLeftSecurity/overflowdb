package overflowdb.util;

import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Function;

public class IteratorUtils {

  public static <A> Iterator<A> fromSingle(A a) {
    return new SingleIterator<>(a);
  }

  public static <A> Iterator<A> from (A... as) {
    return Arrays.stream(as).iterator();
  }

  public static final <S, E> Iterator<E> map(final Iterator<S> iterator, final Function<S, E> function) {
    return new Iterator<E>() {
      public boolean hasNext() {
        return iterator.hasNext();
      }
      public void remove() {
        iterator.remove();
      }
      public E next() {
        return function.apply(iterator.next());
      }
    };
  }

  public static class SingleIterator<A> implements Iterator<A> {
    private A element;
    public SingleIterator(A element) {
      this.element = element;
    }

    public boolean hasNext() {
      return element != null;
    }
    public A next() {
      A ret = element;
      element = null; // free for garbage collection
      return ret;
    }
  }
}
