package overflowdb;

import org.junit.Test;
import overflowdb.testdomains.simple.SimpleDomain;
import overflowdb.testdomains.simple.TestEdge;
import overflowdb.testdomains.simple.TestNode;

import static org.junit.Assert.assertEquals;

public class DiffGraphTest {

  @Test
  public void nodeRemovalTest() {
    try (Graph graph = SimpleDomain.newGraph()) {
      BatchedUpdate.DiffGraphBuilder diff1 = new BatchedUpdate.DiffGraphBuilder();
      DetachedNodeData newNode = new DetachedNodeGeneric(TestNode.LABEL, TestNode.STRING_PROPERTY, "node 1");
      diff1.addNode(newNode);
      BatchedUpdate.applyDiff(graph, diff1);
      assertNodeCount(1, graph);
      Node node = graph.nodes().next();

      BatchedUpdate.DiffGraphBuilder diff2 = new BatchedUpdate.DiffGraphBuilder();
      diff2.removeNode(node);
      // it shouldn't matter (and especially not error) if we remove the same node twice
      diff2.removeNode(node);
      BatchedUpdate.applyDiff(graph, diff2);
      assertNodeCount(0, graph);
    }
  }

  @Test
  public void edgeRemovalTest() {
    try (Graph graph = SimpleDomain.newGraph()) {
      BatchedUpdate.DiffGraphBuilder diff1 = new BatchedUpdate.DiffGraphBuilder();
      DetachedNodeData n1D = new DetachedNodeGeneric(TestNode.LABEL, TestNode.STRING_PROPERTY, "node 1");
      DetachedNodeData n2D = new DetachedNodeGeneric(TestNode.LABEL, TestNode.STRING_PROPERTY, "node 2");
      diff1.addEdge(n1D, n2D,TestEdge.LABEL, TestEdge.LONG_PROPERTY, 99L);

      BatchedUpdate.applyDiff(graph, diff1);
      assertNodeCount(2, graph);
      assertEdgeCount(1, graph);
      Edge edge = graph.edges().next();

      BatchedUpdate.DiffGraphBuilder diff2 = new BatchedUpdate.DiffGraphBuilder();
      diff2.removeEdge(edge);
      // it shouldn't matter (and especially not error) if we remove the same edge twice
      diff2.removeEdge(edge);
      BatchedUpdate.applyDiff(graph, diff2);
      assertNodeCount(2, graph);
      assertEdgeCount(0, graph);
    }
  }

  private void assertNodeCount(int expected, Graph graph) {
    assertEquals("node count different to expected", expected, graph.nodeCount());
  }

  private void assertEdgeCount(int expected, Graph graph) {
    assertEquals("edge count different to expected", expected, graph.edgeCount());
  }

}
