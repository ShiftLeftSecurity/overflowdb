package io.shiftleft.overflowdb.storage;

import io.shiftleft.overflowdb.structure.OverflowDbGraph;
import io.shiftleft.overflowdb.structure.specialized.gratefuldead.Artist;
import io.shiftleft.overflowdb.structure.specialized.gratefuldead.FollowedBy;
import io.shiftleft.overflowdb.structure.specialized.gratefuldead.Song;
import io.shiftleft.overflowdb.structure.specialized.gratefuldead.SungBy;
import io.shiftleft.overflowdb.structure.specialized.gratefuldead.WrittenBy;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * save and restore a graph from disk overlay
 */
public class GraphSaveRestoreTest {

  @Test
  @Ignore
  public void greenField() throws IOException {
    final File overflowDb = Files.createTempFile("overflowdb", "bin").toFile();
    overflowDb.deleteOnExit();

    final Long vertex0Id;
    final Long vertex1Id;
    // create graph and store in specified location
    try (OverflowDbGraph graph = newGratefulDeadGraphWithSpecializedElements(overflowDb)) {
      Vertex v0 = graph.addVertex(T.label, Song.label, Song.NAME, "Song 1");
      Vertex v1 = graph.addVertex(T.label, Song.label, Song.NAME, "Song 2");
      Edge edge = v0.addEdge(FollowedBy.LABEL, v1, FollowedBy.WEIGHT, 42);
      vertex0Id = (Long) v0.id();
      vertex1Id = (Long) v1.id();
    } // ARM auto-close will trigger saving to disk because we specified a location

    // reload from disk
    try (OverflowDbGraph graph = newGratefulDeadGraphWithSpecializedElements(overflowDb)) {
      assertEquals(Long.valueOf(2), graph.traversal().V().count().next());
      assertEquals(Long.valueOf(1), graph.traversal().V().outE().count().next());
      assertEquals("Song 1", graph.vertex(vertex0Id).value(Song.NAME));
      assertEquals("Song 2", graph.vertex(vertex1Id).value(Song.NAME));
      assertEquals("Song 2", graph.traversal().V(vertex0Id).out(FollowedBy.LABEL).values(Song.NAME).next());

      // ensure we can add more elements
      Vertex v1 = graph.vertex(vertex1Id);
      Vertex v2 = graph.addVertex(T.label, Song.label, Song.NAME, "Song 3");
      v1.addEdge(FollowedBy.LABEL, v2, FollowedBy.WEIGHT, 43);
      assertEquals("Song 3", graph.traversal().V(vertex0Id).out().out(FollowedBy.LABEL).values(Song.NAME).next());
    }
  }

  @Test
  @Ignore
  public void completeGratefulDeadGraph() throws IOException {
    final File overflowDb = Files.createTempFile("overflowdb", "bin").toFile();
    overflowDb.deleteOnExit();

    try (OverflowDbGraph graph = newGratefulDeadGraphWithSpecializedElements(overflowDb)) {
      loadGraphMl(graph);
    } // ARM auto-close will trigger saving to disk because we specified a location

    // reload from disk
    try (OverflowDbGraph graph = newGratefulDeadGraphWithSpecializedElements(overflowDb)) {
      assertEquals(Long.valueOf(808), graph.traversal().V().count().next());
      assertEquals(Long.valueOf(8049), graph.traversal().V().outE().count().next());
    }
  }

  private OverflowDbGraph newGratefulDeadGraphWithSpecializedElements(File overflowDb) {
    Configuration configuration = OverflowDbGraph.EMPTY_CONFIGURATION();
    configuration.setProperty(OverflowDbGraph.SWAPPING_ENABLED, true);
    configuration.setProperty(OverflowDbGraph.GRAPH_LOCATION, overflowDb.getAbsolutePath());
    return OverflowDbGraph.open(
        configuration,
        Arrays.asList(Song.factory, Artist.factory),
        Arrays.asList(FollowedBy.factory, SungBy.factory, WrittenBy.factory)
    );
  }

  private void loadGraphMl(OverflowDbGraph graph) throws IOException {
    graph.io(IoCore.graphml()).readGraph("src/test/resources/grateful-dead.xml");
  }

}