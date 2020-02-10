package overflowdb;

import overflowdb.testdomains.gratefuldead.GratefulDead;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.util.TimeUtil;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IndexesTest {

  @Test
  @Ignore // only run manually since the timings vary depending on the environment
  public void shouldUseIndices() throws IOException {
    int loops = 100_000;
    final double avgTimeWithIndex;
    final double avgTimeWithoutIndex;

    { // tests with index
      OdbGraph graph = GratefulDead.newGraphWithData();
      graph.indexManager.createNodePropertyIndex("performances");
      GraphTraversalSource g = graph.traversal();
      assertEquals(142, (long) g.V().has("performances", P.eq(1)).count().next());
      avgTimeWithIndex = TimeUtil.clock(loops, () -> g.V().has("performances", P.eq(1)).count().next());
      graph.close();
    }

    { // tests without index
      OdbGraph graph = GratefulDead.newGraphWithData();
      GraphTraversalSource g = graph.traversal();
      assertEquals(142, (long) g.V().has("performances", P.eq(1)).count().next());
      avgTimeWithoutIndex = TimeUtil.clock(loops, () -> g.V().has("performances", P.eq(1)).count().next());
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
    int loops = 100_000;
    final double avgTimeWithIndex;
    final double avgTimeWithoutIndex;

    { // tests with index
      OdbGraph graph = GratefulDead.newGraph();
      graph.indexManager.createNodePropertyIndex("performances");
      GratefulDead.loadData(graph);
      GraphTraversalSource g = graph.traversal();
      assertEquals(142, (long) g.V().has("performances", P.eq(1)).count().next());
      avgTimeWithIndex = TimeUtil.clock(loops, () -> g.V().has("performances", P.eq(1)).count().next());
      graph.close();
    }

    { // tests without index
      OdbGraph graph = GratefulDead.newGraphWithData();
      GraphTraversalSource g = graph.traversal();
      assertEquals(142, (long) g.V().has("performances", P.eq(1)).count().next());
      avgTimeWithoutIndex = TimeUtil.clock(loops, () -> g.V().has("performances", P.eq(1)).count().next());
      graph.close();
    }

    System.out.println("avgTimeWithIndex = " + avgTimeWithIndex);
    System.out.println("avgTimeWithoutIndex = " + avgTimeWithoutIndex);
    assertTrue("avg time with index should be (significantly) less than without index",
        avgTimeWithIndex < avgTimeWithoutIndex);
  }

  @Test
  @Ignore // only run manually since the timings vary depending on the environment
  public void testPerformanceOfStoredIndices() throws IOException {
    final double avgTimeWithIndexCreation;
    final double avgTimeWithStoredIndex;
    final File overflowDb = Files.createTempFile("overflowdb", "bin").toFile();
    overflowDb.deleteOnExit();
    // tests with index
    int loops = 1000;
    avgTimeWithIndexCreation = TimeUtil.clock(loops, () -> {
      try(OdbGraph graph = GratefulDead.newGraphWithData()) {
        graph.indexManager.createNodePropertyIndex("performances");
        GraphTraversalSource g = graph.traversal();
        assertEquals(142, (long) g.V().has("performances", P.eq(1)).count().next());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    // save indexes
    try(OdbGraph graph = GratefulDead.newGraphWithData(overflowDb.getAbsolutePath())) {
      graph.indexManager.createNodePropertyIndex("performances");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    // tests with stored index
    avgTimeWithStoredIndex = TimeUtil.clock(loops, () -> {
      try(OdbGraph graph = GratefulDead.newGraph(OdbConfig.withDefaults().withStorageLocation(overflowDb.getAbsolutePath()))) {
        GraphTraversalSource g = graph.traversal();
        assertEquals(142, (long) g.V().has("performances", P.eq(1)).count().next());
      }
    });

    System.out.println("avgTimeWithIndexCreation = " + avgTimeWithIndexCreation);
    System.out.println("avgTimeWithStoredIndex = " + avgTimeWithStoredIndex);
  }

  @Test
  public void shouldStoreAndRestoreIndexes() throws IOException {
    final File overflowDb = Files.createTempFile("overflowdb", "bin").toFile();
    overflowDb.deleteOnExit();
    // save indexes
    try(OdbGraph graph = GratefulDead.newGraphWithData(overflowDb.getAbsolutePath())) {
      graph.indexManager.createNodePropertyIndex("performances");
      assertEquals(584, graph.indexManager.getIndexedNodeCount("performances"));
      assertEquals(new HashSet<String>(Arrays.asList("performances")), graph.indexManager.getIndexedNodeProperties());
    }
    // tests with stored index
    try(OdbGraph graph = GratefulDead.newGraph(OdbConfig.withDefaults().withStorageLocation(overflowDb.getAbsolutePath()))) {
      assertEquals(584, graph.indexManager.getIndexedNodeCount("performances"));
      assertEquals(new HashSet<String>(Arrays.asList("performances")), graph.indexManager.getIndexedNodeProperties());
    }
  }

}
