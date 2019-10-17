package io.shiftleft.overflowdb;

import io.shiftleft.overflowdb.testdomains.simple.SimpleDomain;
import io.shiftleft.overflowdb.testdomains.simple.TestEdge;
import io.shiftleft.overflowdb.testdomains.simple.TestNode;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;

public class GraphBuilderTest {
  @Test
  public void shouldAllowToBuildGraphInTransaction() {
    OdbConfig config = OdbConfig.withoutOverflow();
    int[] ints = {1, 2, 3};
    try (OdbGraph graph = SimpleDomain.newGraph(config)) {
      final OdbGraphBuilder builder = graph.createGraphBuilder();
      long v0 = builder.addVertex(T.label, TestNode.LABEL, TestNode.INT_LIST_PROPERTY, Arrays.asList(ints));
      long v1 = builder.addVertex(T.label, TestNode.LABEL, TestNode.INT_PROPERTY, new Integer(123));
      builder.addEdge(v0, v1, TestEdge.LABEL);
      builder.addEdgeProperty(v0, v1, TestEdge.LABEL, TestEdge.LONG_PROPERTY, 99L);
      builder.appendToGraph();
      Assert.assertEquals(2, graph.nodes.size());
      Assert.assertEquals(TestNode.INT_LIST_PROPERTY, graph.vertex(v0).property(TestNode.INT_LIST_PROPERTY).key());
      Assert.assertEquals(Arrays.asList(ints), graph.vertex(v0).property(TestNode.INT_LIST_PROPERTY).value());
      Assert.assertEquals(TestNode.INT_PROPERTY, graph.vertex(v1).property(TestNode.INT_PROPERTY).key());
      Assert.assertEquals(123, graph.vertex(v1).property(TestNode.INT_PROPERTY).value());
      Assert.assertEquals(TestEdge.LABEL, graph.vertex(v0).edges(Direction.OUT, TestEdge.LABEL).next().label());
      Assert.assertEquals(99L, graph.vertex(v0).edges(Direction.OUT, TestEdge.LABEL).next().property(TestEdge.LONG_PROPERTY).value());
      Assert.assertEquals(TestEdge.LABEL, graph.vertex(v1).edges(Direction.IN, TestEdge.LABEL).next().label());
      Assert.assertEquals(99L, graph.vertex(v1).edges(Direction.IN, TestEdge.LABEL).next().property(TestEdge.LONG_PROPERTY).value());
    }
  }
}
