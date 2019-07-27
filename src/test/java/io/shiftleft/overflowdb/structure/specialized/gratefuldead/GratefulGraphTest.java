package io.shiftleft.overflowdb.structure.specialized.gratefuldead;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import io.shiftleft.overflowdb.structure.TinkerGraph;
import io.shiftleft.overflowdb.structure.VertexRef;
import org.apache.tinkerpop.gremlin.util.TimeUtil;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.__;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GratefulGraphTest {

  @Test
  public void simpleTest() {
    TinkerGraph graph = newGratefulDeadGraphWithSpecializedElements();

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
  public void shouldAllowToSpecifyIds() {
    TinkerGraph graph = newGratefulDeadGraphWithSpecializedElements();

    Vertex v10 = graph.addVertex(T.id, 10l, T.label, Song.label, Song.NAME, "Song 10");
    Vertex v20 = graph.addVertex(T.id, 20l, T.label, Song.label, Song.NAME, "Song 20");
    v10.addEdge(FollowedBy.LABEL, v20, FollowedBy.WEIGHT, 5);

    assertEquals(5, graph.traversal().V(10l).outE(FollowedBy.LABEL).values(FollowedBy.WEIGHT).next());
    assertEquals(5, graph.traversal().V(20l).inE(FollowedBy.LABEL).values(FollowedBy.WEIGHT).next());
  }

  @Test
  public void shouldReturnElementRefs() {
    TinkerGraph graph = newGratefulDeadGraphWithSpecializedElements();

    Vertex v0 = graph.addVertex(T.label, Song.label, Song.NAME, "Song 1");
    Vertex v2 = graph.addVertex(T.label, Song.label, Song.NAME, "Song 2");
    Edge e4 = v0.addEdge(FollowedBy.LABEL, v2);
    assertTrue(v0 instanceof VertexRef);
    assertTrue(v0.vertices(Direction.OUT).next() instanceof VertexRef);
  }

  @Test
  /* ensure these are identical for both ondisk overflow enabled/disabled */
  public void optimizationStrategyAffectedSteps() throws IOException {
    TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();

    // using `g.V().hasLabel(lbl)` optimization
    assertEquals(584, graph.traversal().V().hasLabel(Song.label).toList().size());
    assertEquals(142, graph.traversal().V().has(Song.PERFORMANCES, 1).toList().size());
    assertEquals(142, graph.traversal().V().has(Song.PERFORMANCES, 1).hasLabel(Song.label).toList().size());
    assertEquals(142, graph.traversal().V().hasLabel(Song.label).has(Song.PERFORMANCES, 1).toList().size());
    assertEquals(7047, graph.traversal().V().out().hasLabel(Song.label).toList().size());
    assertEquals(1, graph.traversal().V(800l).hasLabel(Song.label).toList().size());
    assertEquals(5, graph.traversal().V(1l).out().hasLabel(Song.label).toList().size());
    assertEquals(0, graph.traversal().V().hasLabel(Song.label).hasLabel(Artist.label).toList().size());
    assertEquals(808, graph.traversal().V().hasLabel(Song.label, Artist.label).toList().size());
    assertEquals(501, graph.traversal().V().outE().hasLabel(WrittenBy.LABEL).toList().size());
    assertEquals(501, graph.traversal().V().hasLabel(Song.label).outE().hasLabel(WrittenBy.LABEL).toList().size());

    // using `g.E().hasLabel(lbl)` optimization
    assertEquals(8049, graph.traversal().E().toList().size());
    assertEquals(7047, graph.traversal().E().hasLabel(FollowedBy.LABEL).toList().size());
    assertEquals(3564, graph.traversal().E().has(FollowedBy.WEIGHT, 1).toList().size());
    assertEquals(3564, graph.traversal().E().hasLabel(FollowedBy.LABEL).has(FollowedBy.WEIGHT, 1).toList().size());
    assertEquals(3564, graph.traversal().E().has(FollowedBy.WEIGHT, 1).hasLabel(FollowedBy.LABEL).toList().size());
    assertEquals(7047, graph.traversal().E().hasLabel(FollowedBy.LABEL).outV().hasLabel(Song.label).toList().size());
    assertEquals(7548, graph.traversal().E().hasLabel(FollowedBy.LABEL, SungBy.LABEL).toList().size());

    graph.close();
  }

  @Test
  public void gratefulDeadGraph() throws IOException {
    TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();

    List<Vertex> garcias = graph.traversal().V().has("name", "Garcia").toList();
    assertEquals(garcias.size(), 1);
    VertexRef vertexRef = (VertexRef) garcias.get(0); // Tinkergraph returns VertexRefs for overflow
    Artist garcia = (Artist) vertexRef.get(); //it's actually of type `Artist`, not (only) `Vertex`
    assertEquals("Garcia", garcia.getName());
    graph.close();
  }

  @Test
  public void testBasicSteps() throws IOException {
    TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
    Vertex garcia = graph.traversal().V().has("name", "Garcia").next();

    // inE
    assertEquals(4, __(garcia).inE(WrittenBy.LABEL).toList().size());
    assertEquals(4, __(garcia).inE(WrittenBy.LABEL).outV().toList().size());

    // in
    assertEquals(4, __(garcia).in(WrittenBy.LABEL).toList().size());
    List<Vertex> songsWritten = __(garcia).in(WrittenBy.LABEL).has("name", "CREAM PUFF WAR").toList();
    assertEquals(songsWritten.size(), 1);
    VertexRef<Song> songRef = (VertexRef) songsWritten.get(0); //it's actually of type `VertexRef<Song>`, but we can't infer that since it's behind the tinkerpop api
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
    graph.close();
  }

  @Test
  public void shouldAllowAddingElementsAndSettingProperties() throws IOException {
    TinkerGraph graph = newGratefulDeadGraphWithSpecializedElements();

    Vertex song1 = graph.addVertex(Song.label);
    Vertex song2 = graph.addVertex(Song.label);
    song1.property(Song.NAME, "song 1");
    song2.property(Song.NAME, "song 2");

    List<Vertex> vertices = graph.traversal().V().toList();
    assertEquals(2, vertices.size());
    Set<Object> names = graph.traversal().V().values("name").toSet();
    assertTrue(names.contains("song 1"));
    assertTrue(names.contains("song 2"));

    song1.addEdge(FollowedBy.LABEL, song2, FollowedBy.WEIGHT, new Integer(42));
    assertEquals(42, graph.traversal().E().values(FollowedBy.WEIGHT).next());
    assertEquals(42, __(song1).outE().values(FollowedBy.WEIGHT).next());

    graph.close();
  }

  @Test
  public void shouldSupportEdgeRemoval() {
    TinkerGraph graph = newGratefulDeadGraphWithSpecializedElements();
    Vertex song1 = graph.addVertex(Song.label);
    Vertex song2 = graph.addVertex(Song.label);
    Edge followedBy = song1.addEdge(FollowedBy.LABEL, song2);
    assertEquals(2, graph.traversal().V().toList().size());
    assertEquals(1, graph.traversal().E().toList().size());

    followedBy.remove();
    assertEquals(2, graph.traversal().V().toList().size());
    assertEquals(0, graph.traversal().E().toList().size());

    graph.close();
  }

  @Test
  public void shouldSupportVertexRemoval1() {
    TinkerGraph graph = newGratefulDeadGraphWithSpecializedElements();
    Vertex song1 = graph.addVertex(Song.label);
    Vertex song2 = graph.addVertex(Song.label);
    song1.addEdge(FollowedBy.LABEL, song2);
    assertEquals(2, graph.traversal().V().toList().size());
    assertEquals(1, graph.traversal().E().toList().size());

    song1.remove();
    assertEquals(1, graph.traversal().V().toList().size());
    assertEquals(0, graph.traversal().E().toList().size());

    song2.remove();
    assertEquals(0, graph.traversal().V().toList().size());

    graph.close();
  }

  @Test
  public void shouldSupportVertexRemoval2() {
    TinkerGraph graph = newGratefulDeadGraphWithSpecializedElements();
    Vertex song1 = graph.addVertex(Song.label);
    Vertex song2 = graph.addVertex(Song.label);
    song1.addEdge(FollowedBy.LABEL, song2);
    assertEquals(2, graph.traversal().V().toList().size());
    assertEquals(1, graph.traversal().E().toList().size());

    song2.remove();
    assertEquals(1, graph.traversal().V().toList().size());
    assertEquals(0, graph.traversal().E().toList().size());

    song1.remove();
    assertEquals(0, graph.traversal().V().toList().size());

    graph.close();
  }


  @Test
  @Ignore // only run manually since the timings vary depending on the environment
  public void shouldUseIndices() throws IOException {
    int loops = 100;
    Double avgTimeWithIndex = null;
    Double avgTimeWithoutIndex = null;

    { // tests with index
      TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
      graph.createIndex("weight", Edge.class);
      GraphTraversalSource g = graph.traversal();
      assertEquals(3564, (long) g.E().has("weight", P.eq(1)).count().next());
      avgTimeWithIndex = TimeUtil.clock(loops, () -> g.E().has("weight", P.eq(1)).count().next());
      graph.close();
    }

    { // tests without index
      TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
      GraphTraversalSource g = graph.traversal();
      assertEquals(3564, (long) g.E().has("weight", P.eq(1)).count().next());
      avgTimeWithoutIndex = TimeUtil.clock(loops, () -> g.E().has("weight", P.eq(1)).count().next());
      graph.close();
    }

    System.out.println("avgTimeWithIndex = " + avgTimeWithIndex);
    System.out.println("avgTimeWithoutIndex = " + avgTimeWithoutIndex);
    assertTrue("avg time with index should be (significantly) less than without index",
        avgTimeWithIndex < avgTimeWithoutIndex);
  }

  @Test
  @Ignore // only run manually since the timings vary depending on the environment
  public void shouldUseIndicesCreatedBeforeLoadingData() throws IOException {
    int loops = 100;
    Double avgTimeWithIndex = null;
    Double avgTimeWithoutIndex = null;

    { // tests with index
      TinkerGraph graph = newGratefulDeadGraphWithSpecializedElements();
      graph.createIndex("weight", Edge.class);
      loadGraphMl(graph);
      GraphTraversalSource g = graph.traversal();
      assertEquals(3564, (long) g.E().has("weight", P.eq(1)).count().next());
      avgTimeWithIndex = TimeUtil.clock(loops, () -> g.E().has("weight", P.eq(1)).count().next());
      graph.close();
    }

    { // tests without index
      TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
      GraphTraversalSource g = graph.traversal();
      assertEquals(3564, (long) g.E().has("weight", P.eq(1)).count().next());
      avgTimeWithoutIndex = TimeUtil.clock(loops, () -> g.E().has("weight", P.eq(1)).count().next());
      graph.close();
    }

    System.out.println("avgTimeWithIndex = " + avgTimeWithIndex);
    System.out.println("avgTimeWithoutIndex = " + avgTimeWithoutIndex);
    assertTrue("avg time with index should be (significantly) less than without index",
        avgTimeWithIndex < avgTimeWithoutIndex);
  }

  @Test
  public void handleEmptyProperties() throws IOException {
    TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();

    List<Object> props1 = graph.traversal().V().values("foo").toList();
    // results will be empty, but it should't crash. see https://github.com/ShiftLeftSecurity/tinkergraph-gremlin/issues/12
    assertEquals(props1.size(), 0);
    graph.close();
  }

  private TinkerGraph newGratefulDeadGraphWithSpecializedElements() {
    Configuration configuration = TinkerGraph.EMPTY_CONFIGURATION();
    configuration.setProperty(TinkerGraph.SWAPPING_ENABLED, false);
    return TinkerGraph.open(
        configuration,
        Arrays.asList(Song.factory, Artist.factory),
        Arrays.asList(FollowedBy.factory, SungBy.factory, WrittenBy.factory)
    );
  }

  //
  private TinkerGraph newGratefulDeadGraphWithSpecializedElementsWithData() throws IOException {
    TinkerGraph graph = newGratefulDeadGraphWithSpecializedElements();
    loadGraphMl(graph);
    return graph;
  }

  private void loadGraphMl(TinkerGraph graph) throws IOException {
    graph.io(IoCore.graphml()).readGraph("src/test/resources/grateful-dead.xml");
  }

}
