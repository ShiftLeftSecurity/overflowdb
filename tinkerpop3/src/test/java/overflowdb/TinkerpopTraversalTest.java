package overflowdb;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import overflowdb.testdomains.gratefuldead.Artist;
import overflowdb.testdomains.gratefuldead.FollowedBy;
import overflowdb.testdomains.gratefuldead.GratefulDead;
import overflowdb.testdomains.gratefuldead.GratefulDeadTp3;
import overflowdb.testdomains.gratefuldead.Song;
import overflowdb.testdomains.gratefuldead.WrittenBy;
import overflowdb.testdomains.simple.SimpleDomain;
import overflowdb.testdomains.simple.TestEdge;
import overflowdb.testdomains.simple.TestNode;
import overflowdb.tinkerpop.OdbGraphTp3;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.__;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

// TODO move to tinkerpop-subproject once it's factored out
public class TinkerpopTraversalTest {

  @Test
  public void addV() {
    try (OdbGraphTp3 graph = OdbGraphTp3.wrap(SimpleDomain.newGraph())) {
      Vertex vertex = graph.traversal().addV(TestNode.LABEL).next();

      assertEquals(vertex.id(), graph.traversal().V().next().id());
      assertEquals(vertex.id(), graph.traversal().V(vertex.id()).next().id());
      assertEquals(vertex.id(), graph.traversal().V(vertex).next().id());
    }
  }

  @Test
  public void addE() {
    try (OdbGraphTp3 graph = OdbGraphTp3.wrap(SimpleDomain.newGraph())) {
      Vertex v0 = graph.addVertex(TestNode.LABEL);
      Vertex v1 = graph.addVertex(TestNode.LABEL);
      Edge e = graph.traversal()
          .V(v0)
          .addE(TestEdge.LABEL)
          .to(v1)
          .property(TestEdge.LONG_PROPERTY, 99l)
          .next();

      assertEquals(e, v0.edges(Direction.OUT).next());
      assertEquals(Long.valueOf(99), v0.edges(Direction.OUT).next().value(TestEdge.LONG_PROPERTY));
      assertEquals(v1.id(), v0.edges(Direction.OUT).next().inVertex().id());
      assertEquals(v1.id(), v0.vertices(Direction.OUT).next().id());
    }
  }

  @Test
  public void basicOutInSteps() {
    try(OdbGraphTp3 graph = OdbGraphTp3.wrap(GratefulDead.newGraph())) {
      Vertex v0 = graph.addVertex(T.label, Song.label, Song.NAME, "Song 1");
      Vertex v2 = graph.addVertex(T.label, Song.label, Song.NAME, "Song 2");
      v0.addEdge(FollowedBy.LABEL, v2);

      Set<Object> songNames = graph.traversal().V().values(Song.NAME).toSet();
      assertTrue(songNames.contains("Song 1"));
      assertTrue(songNames.contains("Song 2"));

      assertEquals(1, __(v0).bothE().toList().size());
      assertEquals(1, __(v0).bothE(FollowedBy.LABEL).toList().size());
      assertEquals(0, __(v0).bothE("otherLabel").toList().size());
      assertEquals(1, __(v0).out().toList().size());
      assertEquals(0, __(v2).out().toList().size());
      assertEquals(0, __(v0).in().toList().size());
      assertEquals(1, __(v2).in().toList().size());
      assertEquals(1, __(v0).both().toList().size());
      assertEquals(1, __(v2).both().toList().size());
    }
  }

  @Test
  public void testBasicSteps() throws IOException {
    try (OdbGraphTp3 graph = GratefulDeadTp3.openAndLoadSampleData()) {
      Vertex garcia = graph.traversal().V().has("name", "Garcia").next();

      // inE
      assertEquals(4, __(garcia).inE(WrittenBy.LABEL).toList().size());
      assertEquals(4, __(garcia).inE(WrittenBy.LABEL).outV().toList().size());

      // in
      assertEquals(4, __(garcia).in(WrittenBy.LABEL).toList().size());
      List<Vertex> songsWritten = __(garcia).in(WrittenBy.LABEL).has("name", "CREAM PUFF WAR").toList();
      assertEquals(songsWritten.size(), 1);
      Vertex song = songsWritten.get(0);
      assertEquals("CREAM PUFF WAR", song.value(Song.NAME));

      // outE
      assertEquals(1, __(song).outE(WrittenBy.LABEL).toList().size());

      // out
      List<Vertex> songOut = __(song).out(WrittenBy.LABEL).toList();
      assertEquals(1, songOut.size());
      assertEquals(garcia.id(), songOut.get(0).id());

      // bothE
      List<Edge> songBothE = __(song).bothE(WrittenBy.LABEL).toList();
      assertEquals(1, songBothE.size());

      // both
      List<Vertex> songBoth = __(song).both(WrittenBy.LABEL).toList();
      assertEquals(1, songBoth.size());
      assertEquals("Garcia", songBoth.get(0).value(Song.NAME));
    }
  }

  @Test
  public void loadGratefulDeadGraph() throws IOException {
    try(OdbGraphTp3 graph = GratefulDeadTp3.openAndLoadSampleData()) {
      final Vertex node1 = graph.vertices(1).next();
      assertEquals("HEY BO DIDDLEY", node1.values("name").next());

      List<Vertex> garcias = graph.traversal().V().has("name", "Garcia").toList();
      assertEquals(garcias.size(), 1);
      Vertex garcia = garcias.get(0);
      assertEquals("Garcia", garcia.value(Artist.NAME));
    }
  }

  @Test
  public void simpleTest() {
    try (OdbGraphTp3 graph = OdbGraphTp3.wrap(SimpleDomain.newGraph())) {
      Vertex node1 = graph.addVertex(
          T.label,
          TestNode.LABEL,
          TestNode.STRING_PROPERTY, "node 1",
          TestNode.INT_PROPERTY, 42,
          TestNode.STRING_LIST_PROPERTY, Arrays.asList("stringOne", "stringTwo"),
          TestNode.INT_LIST_PROPERTY, Arrays.asList(42, 43));
      Vertex node2 = graph.addVertex(
          T.label,
          TestNode.LABEL,
          TestNode.STRING_PROPERTY, "node 2",
          TestNode.INT_PROPERTY, 52,
          TestNode.STRING_LIST_PROPERTY, Arrays.asList("stringThree", "stringFour"),
          TestNode.INT_LIST_PROPERTY, Arrays.asList(52, 53));
      Edge edge = node1.addEdge(TestEdge.LABEL, node2, TestEdge.LONG_PROPERTY, 99l);

      //  verify that we can cast to our domain-specific nodes/edges
      assertEquals("node 1", node1.value(TestNode.STRING_PROPERTY));
      assertEquals(Integer.valueOf(42), node1.value(TestNode.INT_PROPERTY));
      assertEquals(Long.valueOf(99), edge.value(TestEdge.LONG_PROPERTY));

      // node traversals
      assertSize(1, node1.vertices(Direction.OUT));
      assertSize(0, node1.vertices(Direction.OUT, "otherLabel"));
      assertSize(0, node2.vertices(Direction.OUT));
      assertSize(0, node1.vertices(Direction.IN));
      assertSize(1, node2.vertices(Direction.IN));
      assertSize(1, node1.vertices(Direction.BOTH));
      assertSize(1, node2.vertices(Direction.BOTH));

      // edge traversals
      assertSize(1, node1.edges(Direction.OUT));
      assertEquals(TestEdge.LABEL, node1.edges(Direction.OUT).next().label());
      assertSize(0, node1.edges(Direction.OUT, "otherLabel"));
      assertSize(0, node2.edges(Direction.OUT));
      assertSize(1, node2.edges(Direction.IN));
      assertSize(1, node1.edges(Direction.BOTH));
      assertSize(1, node1.edges(Direction.BOTH, TestEdge.LABEL));
      assertSize(0, node1.edges(Direction.BOTH, "otherLabel"));

      // node properties
      Set stringProperties = graph.traversal().V().values(TestNode.STRING_PROPERTY).toSet();
      assertTrue(stringProperties.contains("node 1"));
      assertTrue(stringProperties.contains("node 2"));
      assertEquals(42, (int) edge.outVertex().value(TestNode.INT_PROPERTY));
      assertEquals(52, (int) edge.inVertex().value(TestNode.INT_PROPERTY));

      // edge properties
      assertEquals(Long.valueOf(99l), edge.value(TestEdge.LONG_PROPERTY));
      assertEquals(99l, (long) node1.edges(Direction.OUT).next().value(TestEdge.LONG_PROPERTY));
      assertEquals(99l, (long) node2.edges(Direction.IN).next().value(TestEdge.LONG_PROPERTY));
    }
  }

  private void assertSize(int expected, Iterator iter) {
    int size = 0;
    while (iter.hasNext()) {
      size++;
      iter.next();
    }
    assertEquals("iterator size different to expected", expected, size);
  }
}
