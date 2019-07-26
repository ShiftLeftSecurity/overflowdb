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
package org.apache.tinkerpop.gremlin.tinkergraph.storage;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerEdge;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerElement;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class OndiskOverflow implements AutoCloseable {
  private final Logger logger = LoggerFactory.getLogger(getClass());
  protected final VertexSerializer vertexSerializer = new VertexSerializer();
  protected final EdgeSerializer edgeSerializer = new EdgeSerializer();
  protected final Optional<VertexDeserializer> vertexDeserializer;
  protected final Optional<EdgeDeserializer> edgeDeserializer;

  private final MVStore mvstore;
  protected final MVMap<Long, byte[]> vertexMVMap;
  protected final MVMap<Long, byte[]> edgeMVMap;
  private boolean closed;

  public static OndiskOverflow createWithTempFile(final VertexDeserializer vertexDeserializer, final EdgeDeserializer edgeDeserializer) {
    return new OndiskOverflow(Optional.empty(), Optional.ofNullable(vertexDeserializer), Optional.ofNullable(edgeDeserializer));
  }

  /** create with specific mvstore file - which may or may not yet exist.
   * mvstoreFile won't be deleted at the end (unlike temp file constructors above) */
  public static OndiskOverflow createWithSpecificLocation(
      final VertexDeserializer vertexDeserializer, final EdgeDeserializer edgeDeserializer, final File mvstoreFile) {
    return new OndiskOverflow(Optional.ofNullable(mvstoreFile), Optional.ofNullable(vertexDeserializer), Optional.ofNullable(edgeDeserializer));
  }

  /** create with specific mvstore file - which may or may not yet exist.
   * mvstoreFile won't be deleted at the end (unlike temp file constructors above) */
  public static OndiskOverflow createWithSpecificLocation(final File mvstoreFile) {
    return new OndiskOverflow(Optional.ofNullable(mvstoreFile), Optional.empty(), Optional.empty());
  }

  private OndiskOverflow(
      final Optional<File> mvstoreFileMaybe,
      final Optional<VertexDeserializer> vertexDeserializer,
      final Optional<EdgeDeserializer> edgeDeserializer) {
    this.vertexDeserializer = vertexDeserializer;
    this.edgeDeserializer = edgeDeserializer;

    final File mvstoreFile;
    if (mvstoreFileMaybe.isPresent()) {
      mvstoreFile = mvstoreFileMaybe.get();
    } else {
      try {
        mvstoreFile = File.createTempFile("mvstore", ".bin");
        mvstoreFile.deleteOnExit();
      } catch (IOException e) {
        throw new RuntimeException("cannot create tmp file for mvstore", e);
      }
    }
    logger.debug("on-disk overflow file: " + mvstoreFile);

    mvstore = new MVStore.Builder().fileName(mvstoreFile.getAbsolutePath()).open();
    vertexMVMap = mvstore.openMap("vertices");
    edgeMVMap = mvstore.openMap("edges");
  }

  public void persist(final Element element) throws IOException {
    if (!closed) {
      final Long id = (Long) element.id();
      if (element instanceof Vertex) {
        vertexMVMap.put(id, vertexSerializer.serialize((Vertex) element));
      } else if (element instanceof Edge) {
        edgeMVMap.put(id, edgeSerializer.serialize((Edge) element));
      } else {
        throw new RuntimeException("unable to serialize " + element + " of type " + element.getClass());
      }
    }
  }

  public <A extends Vertex> A readVertex(final long id) throws IOException {
    return (A) vertexDeserializer.get().deserialize(vertexMVMap.get(id));
  }

  public <A extends Edge> A readEdge(final long id) throws IOException {
    return (A) edgeDeserializer.get().deserialize(edgeMVMap.get(id));
  }

  @Override
  public void close() {
    closed = true;
    logger.info("closing " + getClass().getSimpleName());
    mvstore.close();
  }

  public File getStorageFile() {
    return new File(mvstore.getFileStore().getFileName());
  }

  public void removeVertex(final Long id) {
    vertexMVMap.remove(id);
  }

  public void removeEdge(final Long id) {
    edgeMVMap.remove(id);
  }

  public Set<Map.Entry<Long, byte[]>> allVertices() {
    return vertexMVMap.entrySet();
  }

  public Set<Map.Entry<Long, byte[]>> allEdges() {
    return edgeMVMap.entrySet();
  }

  public VertexSerializer getVertexSerializer() {
    return vertexSerializer;
  }

  public EdgeSerializer getEdgeSerializer() {
    return edgeSerializer;
  }

  public MVMap<Long, byte[]> getVertexMVMap() {
    return vertexMVMap;
  }

  public MVMap<Long, byte[]> getEdgeMVMap() {
    return edgeMVMap;
  }


  public Optional<VertexDeserializer> getVertexDeserializer() {
    return vertexDeserializer;
  }

  public Optional<EdgeDeserializer> getEdgeDeserializer() {
    return edgeDeserializer;
  }
}
