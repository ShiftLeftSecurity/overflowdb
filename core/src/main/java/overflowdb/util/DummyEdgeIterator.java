package overflowdb.util;

import overflowdb.Direction;
import overflowdb.Edge;
import overflowdb.NodeRef;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class DummyEdgeIterator implements Iterator<Edge> {
  private final Object[] array;
  private int current;
  private final int begin;
  private final int exclusiveEnd;
  private final int strideSize;
  private final Direction direction;
  private final String label;
  private final NodeRef thisRef;

  /** used for peeking */
  private Edge nextCached;

  public DummyEdgeIterator(Object[] array, int begin, int exclusiveEnd, int strideSize,
                           Direction direction, String label, NodeRef thisRef) {
    this.array = array;
    this.begin = begin;
    this.current = begin;
    this.exclusiveEnd = exclusiveEnd;
    this.strideSize = strideSize;
    this.direction = direction;
    this.label = label;
    this.thisRef = thisRef;
  }

  @Override
  public final boolean hasNext() {
    return peekNext() != null;
  }

  private Edge peekNext() {
    if (nextCached != null)
      return nextCached;

    /* there may be holes, e.g. if an edge was removed */
    while (current < exclusiveEnd && array[current] == null) {
      current += strideSize;
    }

    if (current < exclusiveEnd) {
      nextCached = readNext();
      current += strideSize;
      return peekNext();
    }

    // we've reached the end
    return null;
  }

  private Edge readNext() {
    NodeRef otherRef = (NodeRef) array[current];
    Edge dummyEdge;
    if (direction == Direction.OUT) {
      dummyEdge = thisRef.get().instantiateDummyEdge(label, thisRef, otherRef);
      dummyEdge.setOutBlockOffset(current - begin);
    } else {
      dummyEdge = thisRef.get().instantiateDummyEdge(label, otherRef, thisRef);
      dummyEdge.setInBlockOffset(current - begin);
    }
    return dummyEdge;
  }

  @Override
  public Edge next() {
    if (hasNext()) {
      Edge ret = peekNext();
      nextCached = null;
      return ret;
    } else {
      throw new NoSuchElementException();
    }
  }
}
