package overflowdb.storage;

import org.junit.Test;
import overflowdb.Edge;
import overflowdb.Node;
import overflowdb.Config;
import overflowdb.Graph;
import overflowdb.NodeFactory;
import overflowdb.NodeLayoutInformation;
import overflowdb.NodeRef;
import overflowdb.testdomains.gratefuldead.Artist;
import overflowdb.testdomains.gratefuldead.FollowedBy;
import overflowdb.testdomains.gratefuldead.GratefulDead;
import overflowdb.testdomains.gratefuldead.Song;
import overflowdb.testdomains.gratefuldead.SongDb;
import overflowdb.testdomains.gratefuldead.SungBy;
import overflowdb.testdomains.gratefuldead.WrittenBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OdbStorageTest {

  @Test
  public void persistToFileIfStorageConfigured() throws IOException {
    final File storageFile = Files.createTempFile("overflowdb", "bin").toFile();
    Config config = Config.withDefaults().withStorageLocation(storageFile.getAbsolutePath());

    // open empty graph, add one node, close graph
    final long song1Id;
    try (Graph graph = GratefulDead.newGraph(config)) {
      assertEquals(0, graph.nodeCount());
      final Node song1 = graph.addNode(Song.label, Song.NAME, "Song 1");
      song1Id = song1.id();
      assertEquals(1, graph.nodeCount());
    } // ARM auto-close will trigger saving to disk because we specified a location

    // reopen graph: node should be there
    try (Graph graph = GratefulDead.newGraph(config)) {
      assertEquals(1, graph.nodeCount());
      final Node song1 = graph.node(song1Id);
      assertEquals("node should have been persisted to disk and reloaded when reopened the graph",
          "Song 1", song1.property(Song.NAME));

      // delete the node, close the graph
      song1.remove();
      assertEquals(0, graph.nodeCount());
    }

    try (Graph graph = GratefulDead.newGraph(config)) {
      assertEquals(0, graph.nodeCount());
    }

    storageFile.delete(); //cleanup after test
  }

  @Test
  public void shouldDeleteTmpStorageIfNoStorageLocationConfigured() {
    final File tmpStorageFile;

    try (Graph graph = GratefulDead.newGraph()) {
      graph.addNode(Song.label, Song.NAME, "Song 1");
      tmpStorageFile = graph.getStorage().getStorageFile();
    } // ARM auto-close will trigger saving to disk because we specified a location

    assertFalse("temp storage file should be deleted on close", tmpStorageFile.exists());
  }

//  @Test
//  public void shouldLoadOldStorageFormatWhenAddingEdgeType() throws IOException {
//    final File storageFile = Files.createTempFile("overflowdb", "bin").toFile();
//    Config config = Config.withDefaults().withStorageLocation(storageFile.getAbsolutePath());
//
//    final long song1Id;
//    { // create graph: song1 --- followedBy --> song2
//      // schema: Song only has one possible edge
//      NodeLayoutInformation songLayoutInformation = new NodeLayoutInformation(
//          1,
//          new HashSet<>(Arrays.asList(Song.NAME)),
//          Arrays.asList(),
//          Arrays.asList());
///*
//          Arrays.asList(FollowedBy.layoutInformation),
//          Arrays.asList(FollowedBy.layoutInformation));
//*/
//
//      NodeFactory<SongDb> customSongFactory = new NodeFactory<SongDb>() {
//        public String forLabel() {
//          return Song.factory.forLabel();
//        }
//        public int forLabelId() {
//          return Song.factory.forLabelId();
//        }
//        public SongDb createNode(NodeRef<SongDb> ref) {
//          return Song.factory.createNode(ref);
//        }
//        public Song createNodeRef(Graph graph, long id) {
//          return (Song) Song.factory.createNodeRef(graph, id);
//        }
//        public NodeLayoutInformation layoutInformation() {
//          return songLayoutInformation;
//        }
//      };
//
//      Graph graph = Graph.open(config, Arrays.asList(customSongFactory), Arrays.asList(FollowedBy.factory));
//      Node song1 = graph.addNode(Song.label, Song.NAME, "Song 1");
//      Node song2 = graph.addNode(Song.label, Song.NAME, "Song 2");
//      song1.addEdge(FollowedBy.LABEL, song2);
//      song1Id = song1.id();
//      graph.close();
//    }
//
//    { // change the schema: add another possible edge - graph should still load!
//      NodeLayoutInformation songLayoutInformation = new NodeLayoutInformation(
//          1,
//          new HashSet<>(Arrays.asList(Song.NAME)),
//          Arrays.asList(SungBy.layoutInformation, WrittenBy.layoutInformation, FollowedBy.layoutInformation),
//          Arrays.asList(FollowedBy.layoutInformation));
//
//      NodeFactory<SongDb> customSongFactory = new NodeFactory<SongDb>() {
//        public String forLabel() {
//          return Song.factory.forLabel();
//        }
//        public int forLabelId() {
//          return Song.factory.forLabelId();
//        }
//        public SongDb createNode(NodeRef<SongDb> ref) {
//          return Song.factory.createNode(ref);
//        }
//        public Song createNodeRef(Graph graph, long id) {
//          return (Song) Song.factory.createNodeRef(graph, id);
//        }
//        public NodeLayoutInformation layoutInformation() {
//          return songLayoutInformation;
//        }
//      };
//
//      Graph graph = Graph.open(config, Arrays.asList(customSongFactory), Arrays.asList(FollowedBy.factory));
////      Graph graph = Graph.open(config, Arrays.asList(Song.factory), Arrays.asList(FollowedBy.factory, SungBy.factory, WrittenBy.factory));
//      Node song1 = graph.node(song1Id);
//      assertEquals("Song 1", ((Song) song1).name());
//      FollowedBy followedBy = (FollowedBy) song1.outE(FollowedBy.LABEL).next();
//      Node song2 = followedBy.inNode();
//      assertEquals("Song 2", ((Song) song2).name());
//      graph.close();
//    }
//
//    storageFile.delete(); //cleanup after test
//  }

  public void shouldLoadOldStorageFormatWhenAddingEdgeType() throws IOException {
    final File storageFile = Files.createTempFile("overflowdb", "bin").toFile();
    Config config = Config.withDefaults().withStorageLocation(storageFile.getAbsolutePath());

    final long song1Id;
    { // create graph: song1 --- followedBy --> song2
      // schema: Song only has one possible edge
      NodeLayoutInformation songLayoutInformation = new NodeLayoutInformation(
          1,
          new HashSet<>(Arrays.asList(Song.NAME)),
          Arrays.asList(),
          Arrays.asList());
/*
          Arrays.asList(FollowedBy.layoutInformation),
          Arrays.asList(FollowedBy.layoutInformation));
*/

      NodeFactory<SongDb> customSongFactory = new NodeFactory<SongDb>() {
        public String forLabel() {
          return Song.factory.forLabel();
        }
        public int forLabelId() {
          return Song.factory.forLabelId();
        }
        public SongDb createNode(NodeRef<SongDb> ref) {
          return Song.factory.createNode(ref);
        }
        public Song createNodeRef(Graph graph, long id) {
          return (Song) Song.factory.createNodeRef(graph, id);
        }
        public NodeLayoutInformation layoutInformation() {
          return songLayoutInformation;
        }
      };

      Graph graph = Graph.open(config, Arrays.asList(customSongFactory), Arrays.asList(FollowedBy.factory));
      Node song1 = graph.addNode(Song.label, Song.NAME, "Song 1");
      Node song2 = graph.addNode(Song.label, Song.NAME, "Song 2");
      song1.addEdge(FollowedBy.LABEL, song2);
      song1Id = song1.id();
      graph.close();
    }

    { // change the schema: add another possible edge - graph should still load!
      NodeLayoutInformation songLayoutInformation = new NodeLayoutInformation(
          1,
          new HashSet<>(Arrays.asList(Song.NAME)),
          Arrays.asList(SungBy.layoutInformation, WrittenBy.layoutInformation, FollowedBy.layoutInformation),
          Arrays.asList(FollowedBy.layoutInformation));

      NodeFactory<SongDb> customSongFactory = new NodeFactory<SongDb>() {
        public String forLabel() {
          return Song.factory.forLabel();
        }
        public int forLabelId() {
          return Song.factory.forLabelId();
        }
        public SongDb createNode(NodeRef<SongDb> ref) {
          return Song.factory.createNode(ref);
        }
        public Song createNodeRef(Graph graph, long id) {
          return (Song) Song.factory.createNodeRef(graph, id);
        }
        public NodeLayoutInformation layoutInformation() {
          return songLayoutInformation;
        }
      };

      Graph graph = Graph.open(config, Arrays.asList(customSongFactory), Arrays.asList(FollowedBy.factory));
//      Graph graph = Graph.open(config, Arrays.asList(Song.factory), Arrays.asList(FollowedBy.factory, SungBy.factory, WrittenBy.factory));
      Node song1 = graph.node(song1Id);
      assertEquals("Song 1", ((Song) song1).name());
      FollowedBy followedBy = (FollowedBy) song1.outE(FollowedBy.LABEL).next();
      Node song2 = followedBy.inNode();
      assertEquals("Song 2", ((Song) song2).name());
      graph.close();
    }

    storageFile.delete(); //cleanup after test
  }

}
