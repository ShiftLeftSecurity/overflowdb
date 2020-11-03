package overflowdb;

import org.junit.Test;
import overflowdb.testdomains.gratefuldead.GratefulDead;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class LibraryVersionsTest {

  @Test
  public void persistsRandomLibraryVersions() {
    Graph graph = GratefulDead.newGraph();
    graph.persistLibraryVersion("example.Foo1", "1.2.3");
    graph.persistLibraryVersion("example.Foo2", "4.5_SNAPSHOT");

    Collection<Map<String, String>> allVersions = graph.getAllLibraryVersions();
    assertEquals(1, allVersions.size());
    Map<String, String> versions = allVersions.iterator().next();
    assertEquals("1.2.3", versions.get("example.Foo1"));
    assertEquals("4.5_SNAPSHOT", versions.get("example.Foo2"));
  }

  @Test
  public void persistsNewSetOfLibraryVersionsAfterRestart() throws IOException {
    final File storageFile = Files.createTempFile("overflowdb", "bin").toFile();
    Config config = Config.withDefaults().withStorageLocation(storageFile.getAbsolutePath());

    Graph graph = GratefulDead.newGraph(config);
    graph.persistLibraryVersion("example.Foo1", "1.2.3");
    graph.persistLibraryVersion("example.Foo2", "4.5_SNAPSHOT");
    graph.close();

    // adding more versions after restart: graph should preserve both old and new set of versions independently
    graph = GratefulDead.newGraph(config);
    graph.persistLibraryVersion("example.Foo1", "1.2.4");
    graph.persistLibraryVersion("example.Foo2", "4.6_SNAPSHOT");
    Collection<Map<String, String>> allVersions = graph.getAllLibraryVersions();
    assertEquals(2, allVersions.size());

    Iterator<Map<String, String>> versionsIter = allVersions.iterator();
    Map<String, String> versions = versionsIter.next();
    assertEquals("1.2.3", versions.get("example.Foo1"));
    assertEquals("4.5_SNAPSHOT", versions.get("example.Foo2"));

    versions = versionsIter.next();
    assertEquals("1.2.4", versions.get("example.Foo1"));
    assertEquals("4.6_SNAPSHOT", versions.get("example.Foo2"));

    graph.close();
  }

}
