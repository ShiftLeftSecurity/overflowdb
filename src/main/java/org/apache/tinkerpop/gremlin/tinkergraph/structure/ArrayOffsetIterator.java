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
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import java.util.Iterator;

class ArrayOffsetIterator<T> implements Iterator<T> {
  private final Object[] array;
  private int current;
  private final int exclusiveEnd;
  private final int strideSize;

  ArrayOffsetIterator(Object[] array, int begin, int exclusiveEnd, int strideSize) {
    this.array = array;
    this.current = begin;
    this.exclusiveEnd = exclusiveEnd;
    this.strideSize = strideSize;
  }

  @Override
  public boolean hasNext() {
    return current < exclusiveEnd;
  }

  @Override
  public T next() {
    T element = (T) array[current];
    current += strideSize;
    return element;
  }
}
