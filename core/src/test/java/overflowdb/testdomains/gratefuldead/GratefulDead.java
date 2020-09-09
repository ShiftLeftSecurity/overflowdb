package overflowdb.testdomains.gratefuldead;

import overflowdb.Config;
import overflowdb.Graph;

import java.util.Arrays;

public class GratefulDead {
  public static Graph newGraph() {
    return newGraph(Config.withoutOverflow());
  }

  public static Graph newGraph(Config config) {
    return Graph.open(
        config,
        Arrays.asList(Song.factory, Artist.factory),
        Arrays.asList(FollowedBy.factory, SungBy.factory, WrittenBy.factory)
    );
  }

}
