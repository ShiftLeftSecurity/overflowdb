package io.shiftleft.overflowdb.structure;

import io.shiftleft.overflowdb.testdomains.gratefuldead.GratefulDead;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.util.TimeUtil;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IndexesTest {

  @Test
  @Ignore // only run manually since the timings vary depending on the environment
  public void shouldUseIndices() throws IOException {
    int loops = 100;
    Double avgTimeWithIndex = null;
    Double avgTimeWithoutIndex = null;

    { // tests with index
      OdbGraph graph = GratefulDead.newGraphWithData();
      graph.createIndex("weight", Edge.class);
      GraphTraversalSource g = graph.traversal();
      assertEquals(3564, (long) g.E().has("weight", P.eq(1)).count().next());
      avgTimeWithIndex = TimeUtil.clock(loops, () -> g.E().has("weight", P.eq(1)).count().next());
      graph.close();
    }

    { // tests without index
      OdbGraph graph = GratefulDead.newGraphWithData();
      GraphTraversalSource g = graph.traversal();
      assertEquals(3564, (long) g.E().has("weight", P.eq(1)).count().next());
      avgTimeWithoutIndex = TimeUtil.clock(loops, () -> g.E().has("weight", P.eq(1)).count().next());
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
    int loops = 100;
    Double avgTimeWithIndex = null;
    Double avgTimeWithoutIndex = null;

    { // tests with index
      OdbGraph graph = GratefulDead.newGraph();
      graph.createIndex("weight", Edge.class);
      GratefulDead.loadData(graph);
      GraphTraversalSource g = graph.traversal();
      assertEquals(3564, (long) g.E().has("weight", P.eq(1)).count().next());
      avgTimeWithIndex = TimeUtil.clock(loops, () -> g.E().has("weight", P.eq(1)).count().next());
      graph.close();
    }

    { // tests without index
      OdbGraph graph = GratefulDead.newGraphWithData();
      GraphTraversalSource g = graph.traversal();
      assertEquals(3564, (long) g.E().has("weight", P.eq(1)).count().next());
      avgTimeWithoutIndex = TimeUtil.clock(loops, () -> g.E().has("weight", P.eq(1)).count().next());
      graph.close();
    }

    System.out.println("avgTimeWithIndex = " + avgTimeWithIndex);
    System.out.println("avgTimeWithoutIndex = " + avgTimeWithoutIndex);
    assertTrue("avg time with index should be (significantly) less than without index",
        avgTimeWithIndex < avgTimeWithoutIndex);
  }


}
