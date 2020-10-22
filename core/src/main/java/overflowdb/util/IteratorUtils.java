package overflowdb.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Predicate;

public class IteratorUtils {

  public static <A> ArrayList<A> toArrayList(Iterator<A> iterator) {
    ArrayList<A> list = new ArrayList<>();
    while (iterator.hasNext()) {
      A next = iterator.next();
      list.add(next);
//      list.add(iterator.next());
    }
    return list;
  }

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

  public static final <S, E> Iterator<E> flatMap(final Iterator<S> iterator, final Function<S, Iterator<E>> function) {
    return new Iterator<E>() {

      private Iterator<E> currentIterator = Collections.emptyIterator();

      @Override
      public boolean hasNext() {
        if (this.currentIterator.hasNext()) {
          return true;
        } else {
          while (iterator.hasNext()) {
            this.currentIterator = function.apply(iterator.next());
            if (this.currentIterator.hasNext())
              return true;
          }
        }
        return false;
      }

      @Override
      public void remove() {
        iterator.remove();
      }

      @Override
      public E next() {
        if (this.hasNext())
          return this.currentIterator.next();
        else
          throw new NoSuchElementException();
      }
    };
  }

  public static final <S> Iterator<S> filter(final Iterator<S> iterator, final Predicate<S> predicate) {
    return new Iterator<S>() {
      S nextResult = null;

      @Override
      public boolean hasNext() {
        if (null != this.nextResult) {
          return true;
        } else {
          advance();
          return null != this.nextResult;
        }
      }

      @Override
      public void remove() {
        iterator.remove();
      }

      @Override
      public S next() {
        try {
          if (null != this.nextResult) {
            return this.nextResult;
          } else {
            advance();
            if (null != this.nextResult)
              return this.nextResult;
            else
              throw new NoSuchElementException();
          }
        } finally {
          this.nextResult = null;
        }
      }

      private final void advance() {
        this.nextResult = null;
        while (iterator.hasNext()) {
          final S s = iterator.next();
          if (predicate.test(s)) {
            this.nextResult = s;
            return;
          }
        }
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
