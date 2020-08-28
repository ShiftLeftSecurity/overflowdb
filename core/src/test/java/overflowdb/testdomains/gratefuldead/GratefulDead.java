package overflowdb.testdomains.gratefuldead;

import overflowdb.Config;
import overflowdb.Graph;

import java.io.IOException;
import java.util.Arrays;

public class GratefulDead {
  public static Graph open() {
    return open(Config.withoutOverflow());
  }

  public static Graph open(Config config) {
    return Graph.open(
        config,
        Arrays.asList(Song.factory, Artist.factory),
        Arrays.asList(FollowedBy.factory, SungBy.factory, WrittenBy.factory)
    );
  }

}
