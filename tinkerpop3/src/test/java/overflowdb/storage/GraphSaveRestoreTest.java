package overflowdb.storage;

import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.junit.Test;
import overflowdb.Node;
import overflowdb.OdbConfig;
import overflowdb.OdbEdgeTp3;
import overflowdb.OdbGraph;
import overflowdb.testdomains.gratefuldead.Artist;
import overflowdb.testdomains.gratefuldead.FollowedBy;
import overflowdb.testdomains.gratefuldead.GratefulDead;
import overflowdb.testdomains.gratefuldead.Song;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * save and restore a graph from disk overlay
 */
public class GraphSaveRestoreTest {

  @Test
  public void greenField() throws IOException {
    final File storageFile = Files.createTempFile("overflowdb", "bin").toFile();
    storageFile.deleteOnExit();

    final Long node0Id;
    final Long node1Id;
    // create graph and store in specified location
    try (OdbGraph graph = openGratefulDeadGraph(storageFile, false)) {
      Node n0 = graph.addNode(Song.label, Song.NAME, "Song 1");
      Node n1 = graph.addNode(Song.label, Song.NAME, "Song 2");
      OdbEdgeTp3 edge = n0.addEdge2(FollowedBy.LABEL, n1, FollowedBy.WEIGHT, 42);
      node0Id = n0.id2();
      node1Id = n1.id2();
    } // ARM auto-close will trigger saving to disk because we specified a location

    // reload from disk
    try (OdbGraph graph = openGratefulDeadGraph(storageFile, false)) {
      assertEquals(2, graph.nodeCount());
      assertEquals(Long.valueOf(1), graph.traversal().V().outE().count().next());
      assertEquals("Song 1", graph.node(node0Id).property2(Song.NAME));
      assertEquals("Song 2", graph.node(node1Id).property2(Song.NAME));
      assertEquals("Song 2", graph.node(node0Id).out(FollowedBy.LABEL).next().property2(Song.NAME));

      // ensure we can add more elements
      Node n1 = graph.node(node1Id);
      Node n2 = graph.addNode(Song.label, Song.NAME, "Song 3");
      n1.addEdge2(FollowedBy.LABEL, n2, FollowedBy.WEIGHT, 43);
      assertEquals("Song 3", graph.node(node0Id).out().next().out(FollowedBy.LABEL).next().property2(Song.NAME));
    }
  }

  @Test
  public void completeGratefulDeadGraph() throws IOException {
    final File storageFile = Files.createTempFile("overflowdb", "bin").toFile();
    storageFile.deleteOnExit();

    try (OdbGraph graph = openGratefulDeadGraph(storageFile, false)) {
      loadGraphMl(graph);
    } // ARM auto-close will trigger saving to disk because we specified a location

    // reload from disk
    try (OdbGraph graph = openGratefulDeadGraph(storageFile, false)) {
      assertEquals(808, graph.nodeCount());
      assertEquals(Long.valueOf(8049), graph.traversal().V().outE().count().next());
    }
  }

  @Test
  public void completeGratefulDeadGraphWithOverflowEnabled() throws IOException {
    final File storageFile = Files.createTempFile("overflowdb", "bin").toFile();
    storageFile.deleteOnExit();

    try (OdbGraph graph = openGratefulDeadGraph(storageFile, true)) {
      loadGraphMl(graph);
    } // ARM auto-close will trigger saving to disk because we specified a location

    // reload from disk
    try (OdbGraph graph = openGratefulDeadGraph(storageFile, true)) {
      assertEquals(808, graph.nodeCount());
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
      Node newSong = graph.addNode(Song.label);
      newSong.setProperty(Song.NAME, "new song");
      Node youngBlood = (Node) graph.traversal().V().has(Song.NAME, "YOUNG BLOOD").next();
      youngBlood.addEdge2(FollowedBy.LABEL, newSong);
      int expectedSerializationCount = 2; // both youngBlood and newSong should be serialized
      return expectedSerializationCount;
    });

    modifyAndCloseGraph(storageFile, graph -> {
      // update node property
      Node newSong = (Node) graph.traversal().V().has(Song.NAME, "new song").next();
      newSong.setProperty(Song.PERFORMANCES, 5);
      int expectedSerializationCount = 1;
      return expectedSerializationCount;
    });

    // TODO implement property removal (both node and edge)
//    modifyAndCloseGraph(storageFile, graph -> {
//      // remove node property
//      Node newSong = (Node) graph.traversal().V().has(Song.NAME, "new song").next();
//      newSong.setProperty(Song.PERFORMANCES).remove();
//      return 1;
//    });

    modifyAndCloseGraph(storageFile, graph -> {
      // update edge property
      Node newSong = (Node) graph.traversal().V().has(Song.NAME, "new song").next();
      OdbEdgeTp3 followedBy = newSong.inE().next();
      followedBy.setProperty(FollowedBy.WEIGHT, 10);
      int expectedSerializationCount = 2; // both youngBlood and newSong should be serialized
      return expectedSerializationCount;
    });

    modifyAndCloseGraph(storageFile, graph -> {
      // remove edge
      Node newSong = (Node) graph.traversal().V().has(Song.NAME, "new song").next();
      OdbEdgeTp3 followedBy = newSong.inE().next();
      followedBy.remove();
      int expectedSerializationCount = 2; // both youngBlood and newSong should be serialized
      return expectedSerializationCount;
    });

    modifyAndCloseGraph(storageFile, graph -> {
      // remove node
      Node newSong = (Node) graph.traversal().V().has(Song.NAME, "new song").next();
      newSong.remove();
      int expectedSerializationCount = 0;
      return expectedSerializationCount;
    });

    // verify that deleted node is actually gone
    OdbGraph graph = openGratefulDeadGraph(storageFile, false);
    assertFalse("node should have been deleted from storage", graph.traversal().V().has(Song.NAME, "new song").hasNext());
  }

  private void modifyAndCloseGraph(File storageFile, Function<OdbGraph, Integer> graphModifications) {
    OdbGraph graph = openGratefulDeadGraph(storageFile, false);
    int expectedSerializationCount = graphModifications.apply(graph);
    graph.close();
    assertEquals(expectedSerializationCount, graph.getStorage().nodeSerializer.getSerializedCount());
  }

  private OdbGraph openGratefulDeadGraph(File overflowDb, boolean enableOverflow) {
    OdbConfig config = enableOverflow ? OdbConfig.withDefaults() : OdbConfig.withoutOverflow();
    config = config.withSerializationStatsEnabled();
    return GratefulDead.open(config.withStorageLocation(overflowDb.getAbsolutePath()));
  }

  private void loadGraphMl(OdbGraph graph) throws RuntimeException {
    try {
      graph.io(IoCore.graphml()).readGraph("../src/test/resources/grateful-dead.xml");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
