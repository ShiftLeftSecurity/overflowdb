package overflowdb;

import org.junit.Test;
import overflowdb.testdomains.simple.SimpleDomain;
import overflowdb.testdomains.simple.TestEdge;
import overflowdb.testdomains.simple.TestNode;

import static org.junit.Assert.assertEquals;

public class GraphCloneTest {

  @Test
  public void shouldDeepCloneGraph() {
    OdbConfig config = OdbConfig.withoutOverflow();
    OdbGraph graph = SimpleDomain.newGraph(config);
    Node n0 = graph.addNode(TestNode.LABEL, TestNode.STRING_PROPERTY, "n0");
    Node n1 = graph.addNode(TestNode.LABEL, TestNode.STRING_PROPERTY, "n1");
    n0.addEdge2(TestEdge.LABEL, n1, TestEdge.LONG_PROPERTY, 3L);

    // copy graph
    OdbGraph graph2 = SimpleDomain.newGraph(config);
    graph.copyTo(graph2);
    assertEquals(graph2.nodeCount(), 2);
    assertEquals(graph2.edgeCount(), 1);
    // verify: structure, ids and properties should identical
    Node n0Copy = graph2.node(n0.id2());
    Node n1Copy = graph2.node(n1.id2());
    OdbEdge eCopy = n0Copy.outE(TestEdge.LABEL).next();
    assertEquals("n0", n0Copy.property2(TestNode.STRING_PROPERTY));
    assertEquals("n1", n1Copy.property2(TestNode.STRING_PROPERTY));
    assertEquals(eCopy.property2(TestEdge.LONG_PROPERTY), new Long(3L));
    assertEquals(n1Copy, n0Copy.out(TestEdge.LABEL).next());

    // change graph2
    n0Copy.setProperty(TestNode.STRING_PROPERTY, "n0Copy");
    eCopy.setProperty(TestEdge.LONG_PROPERTY, 4L);
    Node n3 = graph2.addNode(TestNode.LABEL);
    n0Copy.addEdge2(TestEdge.LABEL, n3);
    assertEquals(graph2.nodeCount(), 3);
    assertEquals(graph2.edgeCount(), 2);
    // verify: original graph should remain untouched
    assertEquals(graph.node(n0.id2()).property2(TestNode.STRING_PROPERTY), "n0");
    assertEquals(graph.nodeCount(), 2);
    assertEquals(graph.edgeCount(), 1);
  }

}
