package overflowdb.testdomains.simple;

import overflowdb.OdbConfig;
import overflowdb.OdbGraph;

import java.util.Arrays;

public class SimpleDomain {
  public static OdbGraph newGraph() {
    return newGraph(OdbConfig.withoutOverflow());
  }

  public static OdbGraph newGraph(OdbConfig config) {
    return OdbGraph.open(
        config,
        Arrays.asList(TestNode.factory),
        Arrays.asList(TestEdge.factory)
    );
  }
}
