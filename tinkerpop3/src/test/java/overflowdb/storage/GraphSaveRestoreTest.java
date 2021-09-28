package overflowdb.storage;

import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.junit.Test;
import overflowdb.Node;
import overflowdb.Config;
import overflowdb.Edge;
import overflowdb.Graph;
import overflowdb.tinkerpop.OdbGraphTp3;
import overflowdb.testdomains.gratefuldead.FollowedBy;
import overflowdb.testdomains.gratefuldead.GratefulDead;
import overflowdb.testdomains.gratefuldead.Song;
import overflowdb.util.IteratorUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * save and restore a graph from disk overlay
 * TODO move to core... cannot do currently because it depends on loading graph from graphml...
 */
public class GraphSaveRestoreTest {

  @Test
  public void greenField() throws IOException {
    final File storageFile = Files.createTempFile("overflowdb", "bin").toFile();
    storageFile.deleteOnExit();

    final Long node0Id;
    final Long node1Id;
    // create graph and store in specified location
    try (Graph graph = openGratefulDeadGraph(storageFile, false)) {
      Node n0 = graph.addNode(Song.label, Song.NAME, "Song 1");
      Node n1 = graph.addNode(Song.label, Song.NAME, "Song 2");
      Edge edge = n0.addEdge(FollowedBy.LABEL, n1, FollowedBy.WEIGHT, 42);
      node0Id = n0.id();
      node1Id = n1.id();
    } // ARM auto-close will trigger saving to disk because we specified a location

    // reload from disk
    try (Graph graph = openGratefulDeadGraph(storageFile, false)) {
      assertEquals(2, graph.nodeCount());
      assertEquals(1, graph.edgeCount());
      assertEquals("Song 1", graph.node(node0Id).property(Song.NAME));
      assertEquals("Song 2", graph.node(node1Id).property(Song.NAME));
      assertEquals("Song 2", graph.node(node0Id).out(FollowedBy.LABEL).next().property(Song.NAME));

      // ensure we can add more elements
      Node n1 = graph.node(node1Id);
      Node n2 = graph.addNode(Song.label, Song.NAME, "Song 3");
      n1.addEdge(FollowedBy.LABEL, n2, FollowedBy.WEIGHT, 43);
      assertEquals("Song 3", graph.node(node0Id).out().next().out(FollowedBy.LABEL).next().property(Song.NAME));
    }
  }

  @Test
  public void completeGratefulDeadGraph() throws IOException {
    final File storageFile = Files.createTempFile("overflowdb", "bin").toFile();
    storageFile.deleteOnExit();

    try (Graph graph = openGratefulDeadGraph(storageFile, false)) {
      loadGraphMl(graph);
    } // ARM auto-close will trigger saving to disk because we specified a location

    // reload from disk
    try (Graph graph = openGratefulDeadGraph(storageFile, false)) {
      assertEquals(808, graph.nodeCount());
      assertEquals(8049, graph.edgeCount());
    }
  }

  @Test
  public void completeGratefulDeadGraphWithOverflowEnabled() throws IOException {
    final File storageFile = Files.createTempFile("overflowdb", "bin").toFile();
    storageFile.deleteOnExit();

    try (Graph graph = openGratefulDeadGraph(storageFile, true)) {
      loadGraphMl(graph);
    } // ARM auto-close will trigger saving to disk because we specified a location

    // reload from disk
    try (Graph graph = openGratefulDeadGraph(storageFile, true)) {
      assertEquals(808, graph.nodeCount());
      assertEquals(8049, graph.edgeCount());
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
      graph.nodes().forEachRemaining(x -> {});
      int expectedSerializationCount = 0;
      return expectedSerializationCount;
    });

    modifyAndCloseGraph(storageFile, graph -> {
      // new node, connected with existing node 'garcia'
      Node newSong = graph.addNode(Song.label);
      newSong.setProperty(Song.NAME, "new song");
      Node youngBlood = getSongs(graph, "YOUNG BLOOD").next();
      youngBlood.addEdge(FollowedBy.LABEL, newSong);
      int expectedSerializationCount = 2; // both youngBlood and newSong should be serialized
      return expectedSerializationCount;
    });

    modifyAndCloseGraph(storageFile, graph -> {
      // update node property
      Node newSong = getSongs(graph, "new song").next();
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
      Node newSong = getSongs(graph, "new song").next();
      Edge followedBy = newSong.inE().next();
      followedBy.setProperty(FollowedBy.WEIGHT, 10);
      int expectedSerializationCount = 2; // both youngBlood and newSong should be serialized
      return expectedSerializationCount;
    });

    modifyAndCloseGraph(storageFile, graph -> {
      // remove edge
      Node newSong = getSongs(graph, "new song").next();
      Edge followedBy = newSong.inE().next();
      followedBy.remove();
      int expectedSerializationCount = 2; // both youngBlood and newSong should be serialized
      return expectedSerializationCount;
    });

    modifyAndCloseGraph(storageFile, graph -> {
      // remove node
      Node newSong = getSongs(graph, "new song").next();
      newSong.remove();
      int expectedSerializationCount = 0;
      return expectedSerializationCount;
    });

    // verify that deleted node is actually gone
    Graph graph = openGratefulDeadGraph(storageFile, false);
    assertFalse("node should have been deleted from storage", getSongs(graph, "new song").hasNext());
  }

  private void modifyAndCloseGraph(File storageFile, Function<Graph, Integer> graphModifications) {
    Graph graph = openGratefulDeadGraph(storageFile, false);
    int expectedSerializationCount = graphModifications.apply(graph);
    graph.close();
    assertEquals(expectedSerializationCount, graph.nodeSerializer.getSerializedCount());
  }

  private Graph openGratefulDeadGraph(File overflowDb, boolean enableOverflow) {
    Config config = enableOverflow ? Config.withDefaults() : Config.withoutOverflow();
    config = config.withSerializationStatsEnabled();
    return GratefulDead.newGraph(config.withStorageLocation(overflowDb.getAbsolutePath()));
  }

  private void loadGraphMl(Graph graph) throws RuntimeException {
    try {
      OdbGraphTp3.wrap(graph).io(IoCore.graphml()).readGraph("src/test/resources/grateful-dead.xml");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Iterator<Node> getSongs(Graph graph, String songName) {
    return IteratorUtils.filter(graph.nodes(Song.label), n -> n.property(Song.NAME).equals(songName));
  }

}
