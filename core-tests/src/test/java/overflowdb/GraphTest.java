package overflowdb;

import org.junit.Test;
import overflowdb.testdomains.gratefuldead.Artist;
import overflowdb.testdomains.gratefuldead.FollowedBy;
import overflowdb.testdomains.gratefuldead.GratefulDead;
import overflowdb.testdomains.gratefuldead.Song;
import overflowdb.testdomains.gratefuldead.WrittenBy;
import overflowdb.testdomains.simple.SimpleDomain;
import overflowdb.testdomains.simple.TestEdge;
import overflowdb.testdomains.simple.TestNode;

import java.util.ArrayList;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class GraphTest {

  public static void main(String[] args) throws InterruptedException {
    Graph.open(Config.withoutOverflow(), new ArrayList<>(), new ArrayList<>());

    Thread.sleep(1000000);
  }

  @Test
  public void elementCounts() {
    Graph graph = GratefulDead.newGraph();
    Node artist1 = graph.addNode(Artist.label);
    Node artist2 = graph.addNode(Artist.label);
    Node song1 = graph.addNode(Song.label);
    Node song2 = graph.addNode(Song.label);
    song1.addEdge(WrittenBy.LABEL, artist1);
    song2.addEdge(WrittenBy.LABEL, artist2);
    song1.addEdge(FollowedBy.LABEL, song2);
    song2.addEdge(FollowedBy.LABEL, song1); //loop :)

    assertEquals(4, graph.nodeCount());
    assertEquals(4, graph.edgeCount());

    Map<String, Integer> edgeCountByLabel = graph.edgeCountByLabel();
    assertEquals(2, edgeCountByLabel.size());
    assertEquals(Integer.valueOf(2), edgeCountByLabel.get(WrittenBy.LABEL));
    assertEquals(Integer.valueOf(2), edgeCountByLabel.get(FollowedBy.LABEL));

    // should still work after element removal

    song2.remove();
    assertEquals(3, graph.nodeCount());
    assertEquals(1, graph.edgeCount());

    Map<String, Integer> nodeCountByLabel = graph.nodeCountByLabel();
    assertEquals(2, nodeCountByLabel.size());
    assertEquals(Integer.valueOf(2), nodeCountByLabel.get(Artist.label));
    assertEquals(Integer.valueOf(1), nodeCountByLabel.get(Song.label));
  }

  @Test
  public void shouldDeepCloneGraph() {
    Config config = Config.withoutOverflow();
    Graph graph = SimpleDomain.newGraph(config);
    Node n0 = graph.addNode(TestNode.LABEL, TestNode.STRING_PROPERTY, "n0");
    Node n1 = graph.addNode(TestNode.LABEL, TestNode.STRING_PROPERTY, "n1");
    n0.addEdge(TestEdge.LABEL, n1, TestEdge.LONG_PROPERTY, 3L);

    // copy graph
    Graph graph2 = SimpleDomain.newGraph(config);
    graph.copyTo(graph2);
    assertEquals(graph2.nodeCount(), 2);
    assertEquals(graph2.edgeCount(), 1);
    // verify: structure, ids and properties should identical
    Node n0Copy = graph2.node(n0.id());
    Node n1Copy = graph2.node(n1.id());
    Edge eCopy = n0Copy.outE(TestEdge.LABEL).next();
    assertEquals("n0", n0Copy.property(TestNode.STRING_PROPERTY));
    assertEquals("n1", n1Copy.property(TestNode.STRING_PROPERTY));
    assertEquals(eCopy.property(TestEdge.LONG_PROPERTY), new Long(3L));
    assertEquals(n1Copy, n0Copy.out(TestEdge.LABEL).next());

    // change graph2
    n0Copy.setProperty(TestNode.STRING_PROPERTY, "n0Copy");
    eCopy.setProperty(TestEdge.LONG_PROPERTY, 4L);
    Node n3 = graph2.addNode(TestNode.LABEL);
    n0Copy.addEdge(TestEdge.LABEL, n3);
    assertEquals(graph2.nodeCount(), 3);
    assertEquals(graph2.edgeCount(), 2);
    // verify: original graph should remain untouched
    assertEquals(graph.node(n0.id()).property(TestNode.STRING_PROPERTY), "n0");
    assertEquals(graph.nodeCount(), 2);
    assertEquals(graph.edgeCount(), 1);
  }

}
