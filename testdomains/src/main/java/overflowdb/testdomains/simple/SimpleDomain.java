package overflowdb.testdomains.simple;

import overflowdb.Config;
import overflowdb.Graph;

import java.util.Arrays;

public class SimpleDomain {

  public static Graph newGraph() {
    return newGraph(Config.withoutOverflow());
  }

  public static Graph newGraph(Config config) {
    return Graph.open(
        config,
        Arrays.asList(TestNode.factory),
        Arrays.asList(TestEdge.factory)
    );
  }
}
