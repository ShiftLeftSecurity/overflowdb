package io.shiftleft.overflowdb.structure;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.__;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OdbGraphNodeTest {

  @Test
  public void simpleTest() {
    OdbGraph graph = newGraph();

    Vertex v0 = graph.addVertex(
        T.label, OdbTestNode.LABEL,
        OdbTestNode.STRING_PROPERTY, "node 1",
        OdbTestNode.INT_PROPERTY, 42,
        OdbTestNode.STRING_LIST_PROPERTY, Arrays.asList("stringOne", "stringTwo"),
        OdbTestNode.INT_LIST_PROPERTY, Arrays.asList(42, 43));
    Vertex v1 = graph.addVertex(
        T.label, OdbTestNode.LABEL,
        OdbTestNode.STRING_PROPERTY, "node 2",
        OdbTestNode.INT_PROPERTY, 52,
        OdbTestNode.STRING_LIST_PROPERTY, Arrays.asList("stringThree", "stringFour"),
        OdbTestNode.INT_LIST_PROPERTY, Arrays.asList(52, 53));
    Edge e = v0.addEdge(OdbTestEdge.LABEL, v1, OdbTestEdge.LONG_PROPERTY, 99l);

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
    assertEquals(OdbTestEdge.LABEL, __(v0).outE().label().next());
    assertEquals(0, __(v0).outE("otherLabel").toList().size());
    assertEquals(0, __(v1).outE().toList().size());
    assertEquals(1, __(v1).inE().toList().size());
    assertEquals(1, __(v0).bothE().toList().size());
    assertEquals(1, __(v0).bothE(OdbTestEdge.LABEL).toList().size());
    assertEquals(0, __(v0).bothE("otherLabel").toList().size());

    // vertex properties
    Set stringProperties = graph.traversal().V().values(OdbTestNode.STRING_PROPERTY).toSet();
    assertTrue(stringProperties.contains("node 1"));
    assertTrue(stringProperties.contains("node 2"));
    assertEquals(Integer.valueOf(42), __(e).outV().values(OdbTestNode.INT_PROPERTY).next());
    assertEquals(Integer.valueOf(52), __(e).inV().values(OdbTestNode.INT_PROPERTY).next());

    // edge properties
    assertTrue(e instanceof OdbTestEdge);
    assertEquals(Long.valueOf(99l), ((OdbTestEdge) e).longProperty());
    assertEquals(Long.valueOf(99l), e.value(OdbTestEdge.LONG_PROPERTY));
    assertEquals(Long.valueOf(99l), __(v0).outE().values(OdbTestEdge.LONG_PROPERTY).next());
    assertEquals(Long.valueOf(99l), __(v1).inE().values(OdbTestEdge.LONG_PROPERTY).next());
    assertEquals(Long.valueOf(99l), __(v1).inE().values().next());
  }

  @Test
  public void testEdgeEquality() {
    OdbGraph graph = newGraph();

    Vertex v0 = graph.addVertex(T.label, OdbTestNode.LABEL);
    Vertex v1 = graph.addVertex(T.label, OdbTestNode.LABEL);

    Edge e0 = v0.addEdge(OdbTestEdge.LABEL, v1, OdbTestEdge.LONG_PROPERTY, 99l);


    Edge e0FromOut = v0.edges(Direction.OUT).next();
    Edge e0FromIn = v1.edges(Direction.IN).next();

    assertEquals(e0, e0FromOut);
    assertEquals(e0, e0FromIn);
    assertEquals(e0FromOut, e0FromIn);
  }

  @Test
  public void setAndGetEdgePropertyViaNewEdge() {
    OdbGraph graph = newGraph();

    Vertex v0 = graph.addVertex(T.label, OdbTestNode.LABEL);
    Vertex v1 = graph.addVertex(T.label, OdbTestNode.LABEL);

    Edge e0 = v0.addEdge(OdbTestEdge.LABEL, v1);
    e0.property(OdbTestEdge.LONG_PROPERTY, 1L);
    assertEquals(Long.valueOf(1L), e0.property(OdbTestEdge.LONG_PROPERTY).value());
  }

  @Test
  public void setAndGetEdgePropertyViaQueriedEdge() {
    OdbGraph graph = newGraph();

    Vertex v0 = graph.addVertex(T.label, OdbTestNode.LABEL);
    Vertex v1 = graph.addVertex(T.label, OdbTestNode.LABEL);

    v0.addEdge(OdbTestEdge.LABEL, v1);

    Edge e0 = v0.edges(Direction.OUT, OdbTestEdge.LABEL).next();
    e0.property(OdbTestEdge.LONG_PROPERTY, 1L);
    assertEquals(Long.valueOf(1L), e0.property(OdbTestEdge.LONG_PROPERTY).value());
  }

  @Test
  public void setAndGetEdgePropertyViaDifferenceQueriedEdges() {
    OdbGraph graph = newGraph();

    Vertex v0 = graph.addVertex(T.label, OdbTestNode.LABEL);
    Vertex v1 = graph.addVertex(T.label, OdbTestNode.LABEL);

    v0.addEdge(OdbTestEdge.LABEL, v1);

    Edge e0ViaOut = v0.edges(Direction.OUT, OdbTestEdge.LABEL).next();
    e0ViaOut.property(OdbTestEdge.LONG_PROPERTY, 1L);

    Edge e0ViaIn = v1.edges(Direction.IN, OdbTestEdge.LABEL).next();
    assertEquals(Long.valueOf(1L), e0ViaIn.property(OdbTestEdge.LONG_PROPERTY).value());
  }

  @Test
  public void setAndGetEdgePropertyViaNewEdgeMultiple() {
    OdbGraph graph = newGraph();

    Vertex v0 = graph.addVertex(T.label, OdbTestNode.LABEL);
    Vertex v1 = graph.addVertex(T.label, OdbTestNode.LABEL);

    Edge e0 = v0.addEdge(OdbTestEdge.LABEL, v1);
    Edge e1 = v0.addEdge(OdbTestEdge.LABEL, v1);

    e0.property(OdbTestEdge.LONG_PROPERTY, 1L);
    e1.property(OdbTestEdge.LONG_PROPERTY, 2L);

    assertEquals(Long.valueOf(1L), e0.property(OdbTestEdge.LONG_PROPERTY).value());
    assertEquals(Long.valueOf(2L), e1.property(OdbTestEdge.LONG_PROPERTY).value());
  }

  @Test
  public void setAndGetEdgePropertyViaQueriedEdgeMultiple() {
    OdbGraph graph = newGraph();

    Vertex v0 = graph.addVertex(T.label, OdbTestNode.LABEL);
    Vertex v1 = graph.addVertex(T.label, OdbTestNode.LABEL);

    v0.addEdge(OdbTestEdge.LABEL, v1);
    v0.addEdge(OdbTestEdge.LABEL, v1);

    Iterator<Edge> edgeIt = v0.edges(Direction.OUT, OdbTestEdge.LABEL);

    Edge e0 = edgeIt.next();
    Edge e1 = edgeIt.next();

    e0.property(OdbTestEdge.LONG_PROPERTY, 1L);
    e1.property(OdbTestEdge.LONG_PROPERTY, 2L);

    assertEquals(Long.valueOf(1L), e0.property(OdbTestEdge.LONG_PROPERTY).value());
    assertEquals(Long.valueOf(2L), e1.property(OdbTestEdge.LONG_PROPERTY).value());
  }

  @Test
  public void setAndGetEdgePropertyViaDifferenceQueriedEdgesMultiple() {
    OdbGraph graph = newGraph();

    Vertex v0 = graph.addVertex(T.label, OdbTestNode.LABEL);
    Vertex v1 = graph.addVertex(T.label, OdbTestNode.LABEL);

    v0.addEdge(OdbTestEdge.LABEL, v1);
    v0.addEdge(OdbTestEdge.LABEL, v1);

    Iterator<Edge> outEdgeIt = v0.edges(Direction.OUT, OdbTestEdge.LABEL);
    Iterator<Edge> inEdgeIt = v1.edges(Direction.IN, OdbTestEdge.LABEL);

    Edge e0ViaOut = outEdgeIt.next();
    Edge e1ViaOut = outEdgeIt.next();
    Edge e0ViaIn = inEdgeIt.next();
    Edge e1ViaIn = inEdgeIt.next();

    e0ViaOut.property(OdbTestEdge.LONG_PROPERTY, 1L);
    e1ViaOut.property(OdbTestEdge.LONG_PROPERTY, 2L);

    assertEquals(Long.valueOf(1L), e0ViaIn.property(OdbTestEdge.LONG_PROPERTY).value());
    assertEquals(Long.valueOf(2L), e1ViaIn.property(OdbTestEdge.LONG_PROPERTY).value());
  }

  @Test
  public void removeEdgeSimple() {
    try (OdbGraph graph = newGraph()) {
      Vertex v0 = graph.addVertex(T.label, OdbTestNode.LABEL, OdbTestNode.STRING_PROPERTY, "v0");
      Vertex v1 = graph.addVertex(T.label, OdbTestNode.LABEL, OdbTestNode.STRING_PROPERTY, "v1");
      Edge edge = v0.addEdge(OdbTestEdge.LABEL, v1, OdbTestEdge.LONG_PROPERTY, 1l);

      edge.remove();

      assertFalse(v0.edges(Direction.OUT).hasNext());
      assertFalse(v1.edges(Direction.IN).hasNext());
    }
  }

  @Test
  public void removeEdgeComplex1() {
    try (OdbGraph graph = newGraph()) {
      Vertex v0 = graph.addVertex(T.label, OdbTestNode.LABEL, OdbTestNode.STRING_PROPERTY, "v0");
      Vertex v1 = graph.addVertex(T.label, OdbTestNode.LABEL, OdbTestNode.STRING_PROPERTY, "v1");
      Edge edge0 = v0.addEdge(OdbTestEdge.LABEL, v1, OdbTestEdge.LONG_PROPERTY, 0l);
      Edge edge1 = v0.addEdge(OdbTestEdge.LABEL, v1, OdbTestEdge.LONG_PROPERTY, 1l);

      edge0.remove();

      Iterator<Edge> v0outEdges = v0.edges(Direction.OUT);
      assertEquals(Long.valueOf(1), v0outEdges.next().value(OdbTestEdge.LONG_PROPERTY));
      assertFalse(v0outEdges.hasNext());
      Iterator<Edge> v1inEdges = v0.edges(Direction.OUT);
      assertEquals(Long.valueOf(1), v1inEdges.next().value(OdbTestEdge.LONG_PROPERTY));
      assertFalse(v1inEdges.hasNext());
    }
  }

  @Test
  public void removeEdgeComplex2() {
    try (OdbGraph graph = newGraph()) {
      Vertex v0 = graph.addVertex(T.label, OdbTestNode.LABEL, OdbTestNode.STRING_PROPERTY, "v0");
      Vertex v1 = graph.addVertex(T.label, OdbTestNode.LABEL, OdbTestNode.STRING_PROPERTY, "v1");
      Edge edge0 = v0.addEdge(OdbTestEdge.LABEL, v1, OdbTestEdge.LONG_PROPERTY, 0l);
      Edge edge1 = v0.addEdge(OdbTestEdge.LABEL, v1, OdbTestEdge.LONG_PROPERTY, 1l);

      edge1.remove();

      Iterator<Edge> v0outEdges = v0.edges(Direction.OUT);
      assertEquals(Long.valueOf(0), v0outEdges.next().value(OdbTestEdge.LONG_PROPERTY));
      assertFalse(v0outEdges.hasNext());
      Iterator<Edge> v1inEdges = v0.edges(Direction.OUT);
      assertEquals(Long.valueOf(0), v1inEdges.next().value(OdbTestEdge.LONG_PROPERTY));
      assertFalse(v1inEdges.hasNext());
    }
  }

  @Test
  public void removeEdgeComplexAfterSerialization() {
    try (OdbGraph graph = newGraph()) {
      Vertex v0 = graph.addVertex(T.label, OdbTestNode.LABEL, OdbTestNode.STRING_PROPERTY, "v0");
      Vertex v1 = graph.addVertex(T.label, OdbTestNode.LABEL, OdbTestNode.STRING_PROPERTY, "v1");
      v0.addEdge(OdbTestEdge.LABEL, v1, OdbTestEdge.LONG_PROPERTY, 0l);
      v0.addEdge(OdbTestEdge.LABEL, v1, OdbTestEdge.LONG_PROPERTY, 1l);

      // round trip serialization, delete edge with longProperty=0;
      graph.referenceManager.clearAllReferences();
      graph.traversal().V(v0.id()).outE().has(OdbTestEdge.LONG_PROPERTY, P.eq(0l)).drop().iterate();

      Iterator<Edge> v0outEdges = v0.edges(Direction.OUT);
      assertEquals(Long.valueOf(1), v0outEdges.next().value(OdbTestEdge.LONG_PROPERTY));
      assertFalse(v0outEdges.hasNext());
      Iterator<Edge> v1inEdges = v1.edges(Direction.IN);
      assertEquals(Long.valueOf(1), v1inEdges.next().value(OdbTestEdge.LONG_PROPERTY));
      assertFalse(v1inEdges.hasNext());
    }
  }

  @Test
  public void removeNodeSimple() {
    try (OdbGraph graph = newGraph()) {
      Vertex v0 = graph.addVertex(T.label, OdbTestNode.LABEL, OdbTestNode.STRING_PROPERTY, "v0");
      Vertex v1 = graph.addVertex(T.label, OdbTestNode.LABEL, OdbTestNode.STRING_PROPERTY, "v1");
      v0.addEdge(OdbTestEdge.LABEL, v1, OdbTestEdge.LONG_PROPERTY, 1l);

      v0.remove();

      assertEquals(Long.valueOf(1), graph.traversal().V().count().next());
      assertFalse(v1.edges(Direction.IN).hasNext());
    }
  }

  private OdbGraph newGraph() {
    return OdbGraph.open(
        OdbConfig.withoutOverflow(),
        Arrays.asList(OdbTestNode.factory),
        Arrays.asList(OdbTestEdge.factory)
    );
  }

}
