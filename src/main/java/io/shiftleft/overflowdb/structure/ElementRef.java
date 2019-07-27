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

import java.io.IOException;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 * Wrapper for an element element, which may be set to `null` by @ReferenceManager to avoid OutOfMemory errors.
 * When it's cleared, it will be persisted to an on-disk storage.
 */
public abstract class ElementRef<E extends Element> implements Element {
  public final long id;

  protected final TinkerGraph graph;
  protected E element;
  private boolean removed = false;

  /** used when creating a element without the underlying element at hand, set element to null
   *  and please ensure it's available on disk */
  public ElementRef(final Object id, final Graph graph, E element) {
    this.id = (long)id;
    this.graph = (TinkerGraph)graph;
    this.element = element;
  }

  public boolean isSet() {
    return element != null;
  }

  public boolean isCleared() {
    return element == null;
  }

  public boolean isRemoved() {
    return removed;
  }

  /* only called by @ReferenceManager */
  protected void clear() throws IOException {
    E ref = element;
    if (ref != null) {
      graph.ondiskOverflow.persist(ref);
    }
    element = null;
  }

  public E get() {
    E ref = element;
    if (ref != null) {
      return ref;
    } else {
      try {
        final E element = readFromDisk(id);
        if (element == null) throw new IllegalStateException("unable to read element from disk; id=" + id);
        this.element = element;
        graph.referenceManager.registerRef(this); // so it can be cleared on low memory
        return element;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void setElement(E e) {
    this.element = e;
  }

  protected abstract E readFromDisk(long elementId) throws IOException;

  @Override
  public Object id() {
    return id;
  }

  @Override
  public Graph graph() {
    return graph;
  }

  // delegate methods start

  @Override
  public void remove() {
    this.removed = true;
    this.get().remove();
  }

  @Override
  public int hashCode() {
    return id().hashCode();
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof ElementRef) {
      return id().equals(((ElementRef) obj).id());
    } else {
      return false;
    }
  }
  // delegate methods end

}
