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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.__;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OverflowDbNodeTest {

  @Test
  public void simpleTest() {
    TinkerGraph graph = newGraph();

    Vertex v0 = graph.addVertex(
        T.label, OverflowDbTestNode.label,
        OverflowDbTestNode.STRING_PROPERTY, "node 1",
        OverflowDbTestNode.INT_PROPERTY, 42,
        OverflowDbTestNode.STRING_LIST_PROPERTY, Arrays.asList("stringOne", "stringTwo"),
        OverflowDbTestNode.INT_LIST_PROPERTY, Arrays.asList(42, 43));
    Vertex v1 = graph.addVertex(
        T.label, OverflowDbTestNode.label,
        OverflowDbTestNode.STRING_PROPERTY, "node 2",
        OverflowDbTestNode.INT_PROPERTY, 52,
        OverflowDbTestNode.STRING_LIST_PROPERTY, Arrays.asList("stringThree", "stringFour"),
        OverflowDbTestNode.INT_LIST_PROPERTY, Arrays.asList(52, 53));
    Edge e = v0.addEdge(OverflowDbTestEdge.label, v1, OverflowDbTestEdge.LONG_PROPERTY, 99l);

    // vertex traversals
    assertEquals(1, __(v0).out().toList().size());
    assertEquals(0, __(v0).out("otherLabel").toList().size());
    assertEquals(0, __(v1).out().toList().size());
    assertEquals(0, __(v0).in().toList().size());
    assertEquals(1, __(v1).in().toList().size());
    assertEquals(1, __(v0).both().toList().size());
    assertEquals(1, __(v1).both().toList().size());

    // edge traversals
    assertEquals(1, __(v0).outE().toList().size());
    assertEquals(OverflowDbTestEdge.label, __(v0).outE().label().next());
    assertEquals(0, __(v0).outE("otherLabel").toList().size());
    assertEquals(0, __(v1).outE().toList().size());
    assertEquals(1, __(v1).inE().toList().size());
    assertEquals(1, __(v0).bothE().toList().size());
    assertEquals(1, __(v0).bothE(OverflowDbTestEdge.label).toList().size());
    assertEquals(0, __(v0).bothE("otherLabel").toList().size());

    // vertex properties
    Set stringProperties = graph.traversal().V().values(OverflowDbTestNode.STRING_PROPERTY).toSet();
    assertTrue(stringProperties.contains("node 1"));
    assertTrue(stringProperties.contains("node 2"));
    assertEquals(Integer.valueOf(42), __(e).outV().values(OverflowDbTestNode.INT_PROPERTY).next());
    assertEquals(Integer.valueOf(52), __(e).inV().values(OverflowDbTestNode.INT_PROPERTY).next());

    // edge properties
    assertTrue(e instanceof OverflowDbTestEdge);
    assertEquals(Long.valueOf(99l), ((OverflowDbTestEdge) e).longProperty());
    assertEquals(Long.valueOf(99l), e.value(OverflowDbTestEdge.LONG_PROPERTY));
    assertEquals(Long.valueOf(99l), __(v0).outE().values(OverflowDbTestEdge.LONG_PROPERTY).next());
    assertEquals(Long.valueOf(99l), __(v1).inE().values(OverflowDbTestEdge.LONG_PROPERTY).next());
    assertEquals(Long.valueOf(99l), __(v1).inE().values().next());
  }

  @Test
  public void testEdgeEquality() {
    TinkerGraph graph = newGraph();

    Vertex v0 = graph.addVertex(T.label, OverflowDbTestNode.label);
    Vertex v1 = graph.addVertex(T.label, OverflowDbTestNode.label);

    Edge e0 = v0.addEdge(OverflowDbTestEdge.label, v1, OverflowDbTestEdge.LONG_PROPERTY, 99l);


    Edge e0FromOut = v0.edges(Direction.OUT).next();
    Edge e0FromIn = v1.edges(Direction.IN).next();

    assertEquals(e0, e0FromOut);
    assertEquals(e0, e0FromIn);
    assertEquals(e0FromOut, e0FromIn);
  }

  @Test
  public void setAndGetEdgePropertyViaNewEdge() {
    TinkerGraph graph = newGraph();

    Vertex v0 = graph.addVertex(T.label, OverflowDbTestNode.label);
    Vertex v1 = graph.addVertex(T.label, OverflowDbTestNode.label);

    Edge e0 = v0.addEdge(OverflowDbTestEdge.label, v1);
    e0.property(OverflowDbTestEdge.LONG_PROPERTY, 1L);
    assertEquals(Long.valueOf(1L), e0.property(OverflowDbTestEdge.LONG_PROPERTY).value());
  }

  @Test
  public void setAndGetEdgePropertyViaQueriedEdge() {
    TinkerGraph graph = newGraph();

    Vertex v0 = graph.addVertex(T.label, OverflowDbTestNode.label);
    Vertex v1 = graph.addVertex(T.label, OverflowDbTestNode.label);

    v0.addEdge(OverflowDbTestEdge.label, v1);

    Edge e0 = v0.edges(Direction.OUT, OverflowDbTestEdge.label).next();
    e0.property(OverflowDbTestEdge.LONG_PROPERTY, 1L);
    assertEquals(Long.valueOf(1L), e0.property(OverflowDbTestEdge.LONG_PROPERTY).value());
  }

  @Test
  public void setAndGetEdgePropertyViaDifferenceQueriedEdges() {
    TinkerGraph graph = newGraph();

    Vertex v0 = graph.addVertex(T.label, OverflowDbTestNode.label);
    Vertex v1 = graph.addVertex(T.label, OverflowDbTestNode.label);

    v0.addEdge(OverflowDbTestEdge.label, v1);

    Edge e0ViaOut = v0.edges(Direction.OUT, OverflowDbTestEdge.label).next();
    e0ViaOut.property(OverflowDbTestEdge.LONG_PROPERTY, 1L);

    Edge e0ViaIn = v1.edges(Direction.IN, OverflowDbTestEdge.label).next();
    assertEquals(Long.valueOf(1L), e0ViaIn.property(OverflowDbTestEdge.LONG_PROPERTY).value());
  }

  @Test
  public void setAndGetEdgePropertyViaNewEdgeMultiple() {
    TinkerGraph graph = newGraph();

    Vertex v0 = graph.addVertex(T.label, OverflowDbTestNode.label);
    Vertex v1 = graph.addVertex(T.label, OverflowDbTestNode.label);

    Edge e0 = v0.addEdge(OverflowDbTestEdge.label, v1);
    Edge e1 = v0.addEdge(OverflowDbTestEdge.label, v1);

    e0.property(OverflowDbTestEdge.LONG_PROPERTY, 1L);
    e1.property(OverflowDbTestEdge.LONG_PROPERTY, 2L);

    assertEquals(Long.valueOf(1L), e0.property(OverflowDbTestEdge.LONG_PROPERTY).value());
    assertEquals(Long.valueOf(2L), e1.property(OverflowDbTestEdge.LONG_PROPERTY).value());
  }

  @Test
  public void setAndGetEdgePropertyViaQueriedEdgeMultiple() {
    TinkerGraph graph = newGraph();

    Vertex v0 = graph.addVertex(T.label, OverflowDbTestNode.label);
    Vertex v1 = graph.addVertex(T.label, OverflowDbTestNode.label);

    v0.addEdge(OverflowDbTestEdge.label, v1);
    v0.addEdge(OverflowDbTestEdge.label, v1);

    Iterator<Edge> edgeIt = v0.edges(Direction.OUT, OverflowDbTestEdge.label);

    Edge e0 = edgeIt.next();
    Edge e1 = edgeIt.next();

    e0.property(OverflowDbTestEdge.LONG_PROPERTY, 1L);
    e1.property(OverflowDbTestEdge.LONG_PROPERTY, 2L);

    assertEquals(Long.valueOf(1L), e0.property(OverflowDbTestEdge.LONG_PROPERTY).value());
    assertEquals(Long.valueOf(2L), e1.property(OverflowDbTestEdge.LONG_PROPERTY).value());
  }

  @Test
  public void setAndGetEdgePropertyViaDifferenceQueriedEdgesMultiple() {
    TinkerGraph graph = newGraph();

    Vertex v0 = graph.addVertex(T.label, OverflowDbTestNode.label);
    Vertex v1 = graph.addVertex(T.label, OverflowDbTestNode.label);

    v0.addEdge(OverflowDbTestEdge.label, v1);
    v0.addEdge(OverflowDbTestEdge.label, v1);

    Iterator<Edge> outEdgeIt = v0.edges(Direction.OUT, OverflowDbTestEdge.label);
    Iterator<Edge> inEdgeIt = v1.edges(Direction.IN, OverflowDbTestEdge.label);

    Edge e0ViaOut = outEdgeIt.next();
    Edge e1ViaOut = outEdgeIt.next();
    Edge e0ViaIn = inEdgeIt.next();
    Edge e1ViaIn = inEdgeIt.next();

    e0ViaOut.property(OverflowDbTestEdge.LONG_PROPERTY, 1L);
    e1ViaOut.property(OverflowDbTestEdge.LONG_PROPERTY, 2L);

    assertEquals(Long.valueOf(1L), e0ViaIn.property(OverflowDbTestEdge.LONG_PROPERTY).value());
    assertEquals(Long.valueOf(2L), e1ViaIn.property(OverflowDbTestEdge.LONG_PROPERTY).value());
  }

  @Test
  public void removeEdgeSimple() {
    try (TinkerGraph graph = newGraph()) {
      Vertex v0 = graph.addVertex(T.label, OverflowDbTestNode.label, OverflowDbTestNode.STRING_PROPERTY, "v0");
      Vertex v1 = graph.addVertex(T.label, OverflowDbTestNode.label, OverflowDbTestNode.STRING_PROPERTY, "v1");
      Edge edge = v0.addEdge(OverflowDbTestEdge.label, v1, OverflowDbTestEdge.LONG_PROPERTY, 1l);

      edge.remove();

      assertFalse(v0.edges(Direction.OUT).hasNext());
      assertFalse(v1.edges(Direction.IN).hasNext());
    }
  }

  @Test
  public void removeEdgeComplex() {
    try (TinkerGraph graph = newGraph()) {
      Vertex v0 = graph.addVertex(T.label, OverflowDbTestNode.label, OverflowDbTestNode.STRING_PROPERTY, "v0");
      Vertex v1 = graph.addVertex(T.label, OverflowDbTestNode.label, OverflowDbTestNode.STRING_PROPERTY, "v1");
      Edge edge0 = v0.addEdge(OverflowDbTestEdge.label, v1, OverflowDbTestEdge.LONG_PROPERTY, 0l);
      Edge edge1 = v0.addEdge(OverflowDbTestEdge.label, v1, OverflowDbTestEdge.LONG_PROPERTY, 1l);

      edge0.remove();

      Iterator<Edge> v0outEdges = v0.edges(Direction.OUT);
      assertEquals(Long.valueOf(1), v0outEdges.next().value(OverflowDbTestEdge.LONG_PROPERTY));
      assertFalse(v0outEdges.hasNext());
      Iterator<Edge> v1inEdges = v0.edges(Direction.OUT);
      assertEquals(Long.valueOf(1), v1inEdges.next().value(OverflowDbTestEdge.LONG_PROPERTY));
      assertFalse(v1inEdges.hasNext());
    }
  }

  @Test
  public void removeEdgeComplexAfterSerialization() throws IOException {
    try (TinkerGraph graph = newGraph()) {
      Vertex v0 = graph.addVertex(T.label, OverflowDbTestNode.label, OverflowDbTestNode.STRING_PROPERTY, "v0");
      Vertex v1 = graph.addVertex(T.label, OverflowDbTestNode.label, OverflowDbTestNode.STRING_PROPERTY, "v1");
      v0.addEdge(OverflowDbTestEdge.label, v1, OverflowDbTestEdge.LONG_PROPERTY, 0l);
      v0.addEdge(OverflowDbTestEdge.label, v1, OverflowDbTestEdge.LONG_PROPERTY, 1l);

      // round trip serialization, delete edge with longProperty=0;
      graph.referenceManager.clearAllReferences();
      graph.traversal().V(v0.id()).outE().has(OverflowDbTestEdge.LONG_PROPERTY, P.eq(0l)).drop().iterate();

      Iterator<Edge> v0outEdges = v0.edges(Direction.OUT);
      assertEquals(Long.valueOf(1), v0outEdges.next().value(OverflowDbTestEdge.LONG_PROPERTY));
      assertFalse(v0outEdges.hasNext());
      Iterator<Edge> v1inEdges = v1.edges(Direction.IN);
      assertEquals(Long.valueOf(1), v1inEdges.next().value(OverflowDbTestEdge.LONG_PROPERTY));
      assertFalse(v1inEdges.hasNext());
    }
  }

  @Test
  public void removeNodeSimple() {
    try (TinkerGraph graph = newGraph()) {
      Vertex v0 = graph.addVertex(T.label, OverflowDbTestNode.label, OverflowDbTestNode.STRING_PROPERTY, "v0");
      Vertex v1 = graph.addVertex(T.label, OverflowDbTestNode.label, OverflowDbTestNode.STRING_PROPERTY, "v1");
      v0.addEdge(OverflowDbTestEdge.label, v1, OverflowDbTestEdge.LONG_PROPERTY, 1l);

      v0.remove();

      assertEquals(Long.valueOf(1), graph.traversal().V().count().next());
      assertFalse(v1.edges(Direction.IN).hasNext());
    }
  }

  private TinkerGraph newGraph() {
    return TinkerGraph.open(
        Arrays.asList(OverflowDbTestNode.factory),
        Arrays.asList(OverflowDbTestEdge.factory)
    );
  }

}
