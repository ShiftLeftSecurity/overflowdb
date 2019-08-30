package io.shiftleft.overflowdb.testdomains.gratefuldead;

import io.shiftleft.overflowdb.OdbConfig;
import io.shiftleft.overflowdb.OdbGraph;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;

import java.io.IOException;
import java.util.Arrays;

public class GratefulDead {
  public static OdbGraph newGraph() {
    return newGraph(OdbConfig.withoutOverflow());
  }

  public static OdbGraph newGraph(OdbConfig config) {
    return OdbGraph.open(
        config,
        Arrays.asList(Song.factory, Artist.factory),
        Arrays.asList(FollowedBy.factory, SungBy.factory, WrittenBy.factory)
    );
  }

}
