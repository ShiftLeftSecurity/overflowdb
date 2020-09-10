package overflowdb.storage;

import org.junit.Test;
import overflowdb.Node;
import overflowdb.Config;
import overflowdb.Graph;
import overflowdb.testdomains.gratefuldead.GratefulDead;
import overflowdb.testdomains.gratefuldead.Song;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

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

}
