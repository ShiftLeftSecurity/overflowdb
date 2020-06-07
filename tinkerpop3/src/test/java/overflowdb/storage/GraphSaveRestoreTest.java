package overflowdb.storage;

import overflowdb.OdbConfig;
import overflowdb.OdbGraph;
import overflowdb.testdomains.gratefuldead.Artist;
import overflowdb.testdomains.gratefuldead.FollowedBy;
import overflowdb.testdomains.gratefuldead.GratefulDead;
import overflowdb.testdomains.gratefuldead.Song;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;

/**
 * save and restore a graph from disk overlay
 */
public class GraphSaveRestoreTest {

  @Test
  public void greenField() throws IOException {
    final File storageFile = Files.createTempFile("overflowdb", "bin").toFile();
    storageFile.deleteOnExit();

    final Long vertex0Id;
    final Long vertex1Id;
    // create graph and store in specified location
    try (OdbGraph graph = newGratefulDeadGraph(storageFile, false)) {
      Vertex v0 = graph.addVertex(T.label, Song.label, Song.NAME, "Song 1");
      Vertex v1 = graph.addVertex(T.label, Song.label, Song.NAME, "Song 2");
      Edge edge = v0.addEdge(FollowedBy.LABEL, v1, FollowedBy.WEIGHT, 42);
      vertex0Id = (Long) v0.id();
      vertex1Id = (Long) v1.id();
    } // ARM auto-close will trigger saving to disk because we specified a location

    // reload from disk
    try (OdbGraph graph = newGratefulDeadGraph(storageFile, false)) {
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
  public void completeGratefulDeadGraph() throws IOException {
    final File storageFile = Files.createTempFile("overflowdb", "bin").toFile();
    storageFile.deleteOnExit();

    try (OdbGraph graph = newGratefulDeadGraph(storageFile, false)) {
      loadGraphMl(graph);
    } // ARM auto-close will trigger saving to disk because we specified a location

    // reload from disk
    try (OdbGraph graph = newGratefulDeadGraph(storageFile, false)) {
      assertEquals(Long.valueOf(808), graph.traversal().V().count().next());
      assertEquals(Long.valueOf(8049), graph.traversal().V().outE().count().next());
    }
  }

  @Test
  public void completeGratefulDeadGraphWithOverflowEnabled() throws IOException {
    final File storageFile = Files.createTempFile("overflowdb", "bin").toFile();
    storageFile.deleteOnExit();

    try (OdbGraph graph = newGratefulDeadGraph(storageFile, true)) {
      loadGraphMl(graph);
    } // ARM auto-close will trigger saving to disk because we specified a location

    // reload from disk
    try (OdbGraph graph = newGratefulDeadGraph(storageFile, true)) {
      assertEquals(Long.valueOf(808), graph.traversal().V().count().next());
      assertEquals(Long.valueOf(8049), graph.traversal().V().outE().count().next());
    }
  }

  @Test
  public void shouldOnlySerializeChangedNodes() throws IOException {
    final File storageFile = Files.createTempFile("overflowdb", "bin").toFile();
    storageFile.deleteOnExit();

    modifyAndCloseGraph(storageFile, graph -> {
      // initial import from graphml - should serialize all nodes
      loadGraphMl(graph);
      int expectedSerializationCount = 808;
      return expectedSerializationCount;
    });

    modifyAndCloseGraph(storageFile, graph -> {
      // no changes, not even traversing (and thus not deserializing nodes)
      int expectedSerializationCount = 0;
      return expectedSerializationCount;
    });

    modifyAndCloseGraph(storageFile, graph -> {
      // traversing (and thus deserializing nodes), but making no changes
      graph.traversal().V().has(Artist.NAME, "Garcia").next();
      int expectedSerializationCount = 0;
      return expectedSerializationCount;
    });

    modifyAndCloseGraph(storageFile, graph -> {
      // new node, connected with existing node 'garcia'
      Vertex newSong = graph.addVertex(Song.label);
      newSong.property(Song.NAME, "new song");
      Vertex youngBlood = graph.traversal().V().has(Song.NAME, "YOUNG BLOOD").next();
      youngBlood.addEdge(FollowedBy.LABEL, newSong);
      int expectedSerializationCount = 2; // both youngBlood and newSong should be serialized
      return expectedSerializationCount;
    });

    modifyAndCloseGraph(storageFile, graph -> {
      // update node property
      Vertex newSong = graph.traversal().V().has(Song.NAME, "new song").next();
      newSong.property(Song.PERFORMANCES, 5);
      int expectedSerializationCount = 1;
      return expectedSerializationCount;
    });

    // TODO implement property removal (both node and edge)
//    modifyAndCloseGraph(storageFile, graph -> {
//      // remove node property
//      Vertex newSong = graph.traversal().V().has(Song.NAME, "new song").next();
//      newSong.property(Song.PERFORMANCES).remove();
//      return 1;
//    });

    modifyAndCloseGraph(storageFile, graph -> {
      // update edge property
      Vertex newSong = graph.traversal().V().has(Song.NAME, "new song").next();
      Edge followedBy = newSong.edges(Direction.IN).next();
      followedBy.property(FollowedBy.WEIGHT, 10);
      int expectedSerializationCount = 2; // both youngBlood and newSong should be serialized
      return expectedSerializationCount;
    });

    modifyAndCloseGraph(storageFile, graph -> {
      // remove edge
      Vertex newSong = graph.traversal().V().has(Song.NAME, "new song").next();
      Edge followedBy = newSong.edges(Direction.IN).next();
      followedBy.remove();
      int expectedSerializationCount = 2; // both youngBlood and newSong should be serialized
      return expectedSerializationCount;
    });

    modifyAndCloseGraph(storageFile, graph -> {
      // remove node
      Vertex newSong = graph.traversal().V().has(Song.NAME, "new song").next();
      newSong.remove();
      int expectedSerializationCount = 1;
      return expectedSerializationCount;
    });
  }

  private void modifyAndCloseGraph(File storageFile, Function<OdbGraph, Integer> graphModifications) {
    OdbGraph graph = newGratefulDeadGraph(storageFile, false);
    int expectedSerializationCount = graphModifications.apply(graph);
    graph.close();
    assertEquals(expectedSerializationCount, graph.getStorage().nodeSerializer.getSerializedCount());
  }

  private OdbGraph newGratefulDeadGraph(File overflowDb, boolean enableOverflow) {
    OdbConfig config = enableOverflow ? OdbConfig.withDefaults() : OdbConfig.withoutOverflow();
    config = config.withSerializationStatsEnabled();
    return GratefulDead.newGraph(config.withStorageLocation(overflowDb.getAbsolutePath()));
  }

  private void loadGraphMl(OdbGraph graph) throws RuntimeException {
    try {
      graph.io(IoCore.graphml()).readGraph("../src/test/resources/grateful-dead.xml");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
