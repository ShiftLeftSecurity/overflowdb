package io.shiftleft.overflowdb.testdomains.simple;

import io.shiftleft.overflowdb.OdbConfig;
import io.shiftleft.overflowdb.OdbGraph;

import java.util.Arrays;

public class SimpleDomain {
  public static OdbGraph newGraph() {
    return newGraph(OdbConfig.withoutOverflow());
  }

  public static OdbGraph newGraph(OdbConfig config) {
    return OdbGraph.open(
        config,
        Arrays.asList(OdbTestNode.factory),
        Arrays.asList(OdbTestEdge.factory)
    );
  }
}
