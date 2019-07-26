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
package org.apache.tinkerpop.gremlin.tinkergraph.storage.iterator;

import gnu.trove.iterator.TLongIterator;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;

import java.util.List;

/**
 * A TLongIterator backed by an Array.
 * Technically this is nonsense - why would you use an Iterator it holds a element to all the data?
 * Since java arrays don't implement `Iterator`, I didn't find a better way.
 * */
public class ArrayBackedTLongIterator implements TLongIterator {

  private final long[] array;
  private int cursor = 0;

  public ArrayBackedTLongIterator(long[] array) {
    this.array = array;
  }

  /* for better performance, use the `long[]` alternative, since it doesn't require boxing/unboxing */
  public ArrayBackedTLongIterator(Long[] array) {
    this.array = new long[array.length];
    for (int i = 0; i < array.length; i++) {
      this.array[i] = array[i];
    }
  }

  @Override
  public boolean hasNext() {
    return array.length > cursor;
  }

  @Override
  public long next() {
    return array[cursor++];
  }

  @Override
  public void remove() {
    throw new NotImplementedException("");
  }

}
