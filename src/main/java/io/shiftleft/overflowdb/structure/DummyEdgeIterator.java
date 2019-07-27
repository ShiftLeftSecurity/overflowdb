package io.shiftleft.overflowdb.structure;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.Iterator;
import java.util.NoSuchElementException;

class DummyEdgeIterator implements Iterator<Edge> {
  private final Object[] array;
  private int current;
  private final int begin;
  private final int exclusiveEnd;
  private final int strideSize;
  private final Direction direction;
  private final String label;
  private final NodeRef<OverflowDbNode> thisRef;

  public DummyEdgeIterator(Object[] array, int begin, int exclusiveEnd, int strideSize,
                    Direction direction, String label, NodeRef<OverflowDbNode> thisRef) {
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
  public boolean hasNext() {
    /* there may be holes, e.g. if an edge was removed */
    while (current < exclusiveEnd && array[current] == null) {
      current += strideSize;
    }
    return current < exclusiveEnd;
  }

  @Override
  public Edge next() {
    if (!hasNext()) throw new NoSuchElementException();

    NodeRef<OverflowDbNode> otherRef = (NodeRef<OverflowDbNode>) array[current];
    OverflowDbEdge dummyEdge;
    if (direction == Direction.OUT) {
      dummyEdge = thisRef.element.instantiateDummyEdge(label, thisRef, otherRef);
      dummyEdge.setOutBlockOffset(current - begin);
    } else {
      dummyEdge = thisRef.element.instantiateDummyEdge(label, otherRef, thisRef);
      dummyEdge.setInBlockOffset(current - begin);
    }
    current += strideSize;
    return dummyEdge;
  }
}
