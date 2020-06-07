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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OdbStorageTest {

  @Test
  public void persistToFileIfStorageConfigured() throws IOException {
    final File storageFile = Files.createTempFile("overflowdb", "bin").toFile();
    OdbConfig config = OdbConfig.withDefaults().withStorageLocation(storageFile.getAbsolutePath());

    try (OdbGraph graph = GratefulDead.newGraph(config)) {
      graph.addVertex(T.label, Song.label, Song.NAME, "Song 1");
    } // ARM auto-close will trigger saving to disk because we specified a location

    assertTrue("storage should be persistent", storageFile.exists());

    storageFile.delete(); //cleanup after test
  }

  @Test
  public void shouldDeleteTmpStorageIfNoStorageLocationConfigured() {
    final File tmpStorageFile;

    try (OdbGraph graph = GratefulDead.newGraph()) {
      graph.addVertex(T.label, Song.label, Song.NAME, "Song 1");
      tmpStorageFile = graph.getStorage().getStorageFile();
    } // ARM auto-close will trigger saving to disk because we specified a location

    assertFalse("temp storage file should be deleted on close", tmpStorageFile.exists());
  }

}
