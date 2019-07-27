/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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
  private final VertexRef<OverflowDbNode> thisRef;

  public DummyEdgeIterator(Object[] array, int begin, int exclusiveEnd, int strideSize,
                    Direction direction, String label, VertexRef<OverflowDbNode> thisRef) {
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

    VertexRef<OverflowDbNode> otherRef = (VertexRef<OverflowDbNode>) array[current];
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
