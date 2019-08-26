package io.shiftleft.overflowdb;

import io.shiftleft.overflowdb.testdomains.gratefuldead.FollowedBy;
import io.shiftleft.overflowdb.testdomains.gratefuldead.GratefulDead;
import io.shiftleft.overflowdb.testdomains.gratefuldead.Song;
import io.shiftleft.overflowdb.testdomains.gratefuldead.WrittenBy;
import io.shiftleft.overflowdb.testdomains.simple.TestEdge;
import io.shiftleft.overflowdb.testdomains.simple.TestNode;
import io.shiftleft.overflowdb.testdomains.simple.SimpleDomain;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.__;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TraversalTest {

  @Test
  public void addV() {
    try (OdbGraph graph = SimpleDomain.newGraph()) {
      Vertex vertex = graph.traversal().addV(TestNode.LABEL).next();

      assertEquals(vertex, graph.traversal().V().next());
      assertEquals(vertex, graph.traversal().V(vertex.id()).next());
      assertEquals(vertex, graph.traversal().V(vertex).next());
    }
  }

  @Test
  public void addE() {
    try (OdbGraph graph = SimpleDomain.newGraph()) {
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
    OdbGraph graph = GratefulDead.newGraph();

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

  @Test
  public void testBasicSteps() throws IOException {
    try (OdbGraph graph = GratefulDead.newGraphWithData()) {
      Vertex garcia = graph.traversal().V().has("name", "Garcia").next();

      // inE
      assertEquals(4, __(garcia).inE(WrittenBy.LABEL).toList().size());
      assertEquals(4, __(garcia).inE(WrittenBy.LABEL).outV().toList().size());

      // in
      assertEquals(4, __(garcia).in(WrittenBy.LABEL).toList().size());
      List<Vertex> songsWritten = __(garcia).in(WrittenBy.LABEL).has("name", "CREAM PUFF WAR").toList();
      assertEquals(songsWritten.size(), 1);
      NodeRef<Song> songRef = (NodeRef) songsWritten.get(0); //it's actually of type `NodeRef<Song>`, but we can't infer that since it's behind the tinkerpop api
      Song song = songRef.get();
      assertEquals("CREAM PUFF WAR", song.getName());

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
    try (OdbGraph graph = GratefulDead.newGraphWithData()) {
      List<Object> props1 = graph.traversal().V().values("foo").toList();
      // results will be empty, but it should't crash. see https://github.com/ShiftLeftSecurity/tinkergraph-gremlin/issues/12
      assertEquals(props1.size(), 0);
    }
  }

}
