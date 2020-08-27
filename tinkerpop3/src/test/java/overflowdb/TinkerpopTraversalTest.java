package overflowdb;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import overflowdb.testdomains.gratefuldead.Artist;
import overflowdb.testdomains.gratefuldead.ArtistDb;
import overflowdb.testdomains.gratefuldead.FollowedBy;
import overflowdb.testdomains.gratefuldead.GratefulDead;
import overflowdb.testdomains.gratefuldead.Song;
import overflowdb.testdomains.gratefuldead.WrittenBy;
import overflowdb.testdomains.simple.SimpleDomain;
import overflowdb.testdomains.simple.TestEdge;
import overflowdb.testdomains.simple.TestNode;

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

      assertEquals(vertex, graph.traversal().V().next());
      assertEquals(vertex, graph.traversal().V(vertex.id()).next());
      assertEquals(vertex, graph.traversal().V(vertex).next());
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
      assertEquals(v1, v0.edges(Direction.OUT).next().inVertex());
      assertEquals(v1, v0.vertices(Direction.OUT).next());
    }
  }

  @Test
  public void basicOutInSteps() {
    try(OdbGraphTp3 graph = OdbGraphTp3.wrap(GratefulDead.open())) {
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
    try (OdbGraphTp3 graph = GratefulDead.openAndLoadSampleData()) {
      Vertex garcia = graph.traversal().V().has("name", "Garcia").next();

      // inE
      assertEquals(4, __(garcia).inE(WrittenBy.LABEL).toList().size());
      assertEquals(4, __(garcia).inE(WrittenBy.LABEL).outV().toList().size());

      // in
      assertEquals(4, __(garcia).in(WrittenBy.LABEL).toList().size());
      List<Vertex> songsWritten = __(garcia).in(WrittenBy.LABEL).has("name", "CREAM PUFF WAR").toList();
      assertEquals(songsWritten.size(), 1);
      Song song = (Song) songsWritten.get(0);
      assertEquals("CREAM PUFF WAR", song.name());

      // outE
      assertEquals(1, __(song).outE(WrittenBy.LABEL).toList().size());

      // out
      List<Vertex> songOut = __(song).out(WrittenBy.LABEL).toList();
      assertEquals(1, songOut.size());
      assertEquals(garcia, songOut.get(0));

      // bothE
      List<Edge> songBothE = __(song).bothE(WrittenBy.LABEL).toList();
      assertEquals(1, songBothE.size());

      // both
      List<Vertex> songBoth = __(song).both(WrittenBy.LABEL).toList();
      assertEquals(1, songBoth.size());
      assertEquals(garcia, songBoth.get(0));
    }
  }

  @Test
  public void handleEmptyProperties() throws IOException {
    try (OdbGraphTp3 graph = GratefulDead.openAndLoadSampleData()) {
      List<Object> props1 = graph.traversal().V().values("foo").toList();
      // results will be empty, but it should't crash. see https://github.com/ShiftLeftSecurity/tinkergraph-gremlin/issues/12
      assertEquals(props1.size(), 0);
    }
  }

  @Test
  public void loadGratefulDeadGraph() throws IOException {
    try(OdbGraphTp3 graph = GratefulDead.openAndLoadSampleData()) {
      final Vertex node1 = graph.vertices(1).next();
      assertEquals("HEY BO DIDDLEY", node1.values("name").next());

      List<Vertex> garcias = graph.traversal().V().has("name", "Garcia").toList();
      assertEquals(garcias.size(), 1);
      Artist artist = (Artist) garcias.get(0);
      ArtistDb garcia = artist.get();
      assertEquals("Garcia", garcia.name());
    }
  }

  @Test
  public void simpleTest() {
    try (OdbGraphTp3 graph = OdbGraphTp3.wrap(SimpleDomain.newGraph())) {
      Vertex n1 = graph.addVertex(
          TestNode.LABEL,
          TestNode.STRING_PROPERTY, "node 1",
          TestNode.INT_PROPERTY, 42,
          TestNode.STRING_LIST_PROPERTY, Arrays.asList("stringOne", "stringTwo"),
          TestNode.INT_LIST_PROPERTY, Arrays.asList(42, 43));
      Vertex n2 = graph.addVertex(
          TestNode.LABEL,
          TestNode.STRING_PROPERTY, "node 2",
          TestNode.INT_PROPERTY, 52,
          TestNode.STRING_LIST_PROPERTY, Arrays.asList("stringThree", "stringFour"),
          TestNode.INT_LIST_PROPERTY, Arrays.asList(52, 53));
      Edge e = n1.addEdge(TestEdge.LABEL, n2, TestEdge.LONG_PROPERTY, 99l);

      //  verify that we can cast to our domain-specific nodes/edges
      TestNode node1 = (TestNode) n1;
      assertEquals("node 1", node1.stringProperty());
      assertEquals(Integer.valueOf(42), node1.intProperty());
      TestEdge testEdge = (TestEdge) e;
      assertEquals(Long.valueOf(99), testEdge.longProperty());

      // node traversals
      assertSize(1, n1.vertices(Direction.OUT));
      assertSize(0, n1.vertices(Direction.OUT, "otherLabel"));
      assertSize(0, n2.vertices(Direction.OUT));
      assertSize(0, n1.vertices(Direction.IN));
      assertSize(1, n2.vertices(Direction.IN));
      assertSize(1, n1.vertices(Direction.BOTH));
      assertSize(1, n2.vertices(Direction.BOTH));

      // edge traversals
      assertSize(1, n1.edges(Direction.OUT));
      assertEquals(TestEdge.LABEL, n1.edges(Direction.OUT).next().label());
      assertSize(0, n1.edges(Direction.OUT, "otherLabel"));
      assertSize(0, n2.edges(Direction.OUT));
      assertSize(1, n2.edges(Direction.IN));
      assertSize(1, n1.edges(Direction.BOTH));
      assertSize(1, n1.edges(Direction.BOTH, TestEdge.LABEL));
      assertSize(0, n1.edges(Direction.BOTH, "otherLabel"));

      // node properties
      // TODO move to tinkerpop-subproject once it's factored out
      Set stringProperties = graph.traversal().V().values(TestNode.STRING_PROPERTY).toSet();
      assertTrue(stringProperties.contains("node 1"));
      assertTrue(stringProperties.contains("node 2"));
      assertEquals(42, (int) e.outVertex().value(TestNode.INT_PROPERTY));
      assertEquals(52, (int) e.inVertex().value(TestNode.INT_PROPERTY));

      // edge properties
      assertTrue(e instanceof TestEdge);
      assertEquals(Long.valueOf(99l), ((TestEdge) e).longProperty());
      assertEquals(Long.valueOf(99l), e.value(TestEdge.LONG_PROPERTY));
      assertEquals(99l, (long) n1.edges(Direction.OUT).next().value(TestEdge.LONG_PROPERTY));
      assertEquals(99l, (long) n2.edges(Direction.IN).next().value(TestEdge.LONG_PROPERTY));
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
