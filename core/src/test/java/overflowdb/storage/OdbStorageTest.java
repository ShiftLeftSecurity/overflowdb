package overflowdb.storage;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
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

  @Test(expected = BackwardsCompatibilityError.class)
  public void shouldErrorWhenTryingToOpenWithoutStorageFormatVersion() throws IOException {
    File storageFile = Files.createTempFile("overflowdb", "bin").toFile();
    storageFile.deleteOnExit();
    OdbStorage storage = OdbStorage.createWithSpecificLocation(storageFile);
    storage.close();

    // modify storage: drop storage version
    MVStore store = new MVStore.Builder().fileName(storageFile.getAbsolutePath()).open();
    final MVMap<String, String> metadata = store.openMap("metadata");
    metadata.remove(OdbStorage.METADATA_KEY_STORAGE_FORMAT_VERSION);
    store.close();

    // should throw a BackwardsCompatibilityError
    OdbStorage.createWithSpecificLocation(storageFile);
  }

  @Test(expected = BackwardsCompatibilityError.class)
  public void shouldErrorWhenTryingToOpenDifferentStorageFormatVersion() throws IOException {
    File storageFile = Files.createTempFile("overflowdb", "bin").toFile();
    storageFile.deleteOnExit();
    OdbStorage storage = OdbStorage.createWithSpecificLocation(storageFile);
    storage.close();

    // modify storage: change storage version
    MVStore store = new MVStore.Builder().fileName(storageFile.getAbsolutePath()).open();
    final MVMap<String, String> metadata = store.openMap("metadata");
    metadata.put(OdbStorage.METADATA_KEY_STORAGE_FORMAT_VERSION, "-1");
    store.close();

    // should throw a BackwardsCompatibilityError
    OdbStorage.createWithSpecificLocation(storageFile);
  }

  @Test
  public void shouldProvideStringToIntGlossary() throws IOException {
    File storageFile = Files.createTempFile("overflowdb", "bin").toFile();
    storageFile.delete();
    storageFile.deleteOnExit();
    OdbStorage storage = OdbStorage.createWithSpecificLocation(storageFile);

    String a = "a";
    String b = "b";
    String c = "c";

    int stringIdA = storage.lookupOrCreateStringToIntMapping(a);
    int stringIdB = storage.lookupOrCreateStringToIntMapping(b);
    assertEquals(a, storage.reverseLookupStringToIntMapping(stringIdA));
    assertEquals(b, storage.reverseLookupStringToIntMapping(stringIdB));

    // should be idempotent - i.e. should not create additional entries
    assertEquals(stringIdA, storage.lookupOrCreateStringToIntMapping(a));
    assertEquals(stringIdB, storage.lookupOrCreateStringToIntMapping(b));

    // should survive restarts
    storage.close();
    storage = OdbStorage.createWithSpecificLocation(storageFile);
    assertEquals(stringIdA, storage.lookupOrCreateStringToIntMapping(a));
    assertEquals(stringIdB, storage.lookupOrCreateStringToIntMapping(b));

    int stringIdC = storage.lookupOrCreateStringToIntMapping(c);
    assertEquals(3, storage.getStringToIntMappings().size());

    assertEquals(a, storage.reverseLookupStringToIntMapping(stringIdA));
    assertEquals(b, storage.reverseLookupStringToIntMapping(stringIdB));
    assertEquals(c, storage.reverseLookupStringToIntMapping(stringIdC));
  }

  @Test
  public void shouldProvideStringToIntGlossary() throws IOException {
    File storageFile = Files.createTempFile("overflowdb", "bin").toFile();
    storageFile.delete();
    storageFile.deleteOnExit();
    OdbStorage storage = OdbStorage.createWithSpecificLocation(storageFile, false);

    String a = "a";
    String b = "b";
    Integer stringIdA = storage.lookupOrCreateGlossaryEntry(a);
    Integer stringIdB = storage.lookupOrCreateGlossaryEntry(b);

    // should be idempotent
    assertEquals(stringIdA, storage.lookupOrCreateGlossaryEntry(a));
    assertEquals(stringIdB, storage.lookupOrCreateGlossaryEntry(b));

    // should survive restarts
    storage.close();
    storage = OdbStorage.createWithSpecificLocation(storageFile, false);
    assertEquals(stringIdA, storage.lookupOrCreateGlossaryEntry(a));
    assertEquals(stringIdB, storage.lookupOrCreateGlossaryEntry(b));

    Integer stringIdC = storage.lookupOrCreateGlossaryEntry("c");
    assertEquals(3, storage.getStringGlossary().size());
  }


}
