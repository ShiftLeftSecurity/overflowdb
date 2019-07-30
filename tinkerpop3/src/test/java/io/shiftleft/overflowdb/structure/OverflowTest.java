package io.shiftleft.overflowdb.structure;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OverflowTest {

//  @Test
  // only run manually since the timings vary depending on the environment
  // without overflow we can hold ~11k nodes in 256m memory
  // with overflow that number should be tremendously larger, because only the reference wrappers are helt in memory
  // it'll be much slower due to the serialization to disk, but should not crash
  // important: use all the following vm opts:  `-Xms256m -Xmx256m`
  public void shouldAllowGraphsLargerThanMemory() throws InterruptedException {
//    boolean enableOverflow = false;
    boolean enableOverflow = true;
    int nodeCount = 100_000;
    int currentInt = 0;
    try(OverflowDbGraph graph = newGraph(enableOverflow)) {
      for (long i = 0; i < nodeCount; i++) {
        if (i % 1000 == 0) {
          System.out.println(i + " nodes created");
        }
        Vertex v = graph.addVertex(OverflowDbTestNode.LABEL);
        List<Integer> ints = new ArrayList<>();
        for (int j = 0; j < 1000; j++) {
          ints.add(currentInt++);
        }
        v.property(OverflowDbTestNode.INT_LIST_PROPERTY, ints);
      }
    }
  }

  private OverflowDbGraph newGraph(boolean enableOverflow) {
    Configuration configuration = OverflowDbGraph.EMPTY_CONFIGURATION();
    configuration.setProperty(OverflowDbGraph.OVERFLOW_ENABLED, enableOverflow);
    return OverflowDbGraph.open(
        configuration,
        Arrays.asList(OverflowDbTestNode.factory),
        Arrays.asList()
    );
  }

}
