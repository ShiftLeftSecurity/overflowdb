package io.shiftleft.overflowdb.structure;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class TraversalTest {

  @Test
  public void addV() {
    try (OdbGraph graph = newGraph()) {
      Vertex vertex = graph.traversal().addV(OdbTestNode.LABEL).next();

      assertEquals(vertex, graph.traversal().V().next());
      assertEquals(vertex, graph.traversal().V(vertex.id()).next());
      assertEquals(vertex, graph.traversal().V(vertex).next());
    }
  }

  @Test
  public void addE() {
    try (OdbGraph graph = newGraph()) {
      Vertex v0 = graph.addVertex(OdbTestNode.LABEL);
      Vertex v1 = graph.addVertex(OdbTestNode.LABEL);
      Edge e = graph.traversal()
          .V(v0)
          .addE(OdbTestEdge.LABEL)
          .to(v1)
          .property(OdbTestEdge.LONG_PROPERTY, 99l)
          .next();

      assertEquals(e, v0.edges(Direction.OUT).next());
      assertEquals(Long.valueOf(99), v0.edges(Direction.OUT).next().value(OdbTestEdge.LONG_PROPERTY));
      assertEquals(v1, v0.edges(Direction.OUT).next().inVertex());
      assertEquals(v1, v0.vertices(Direction.OUT).next());
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
