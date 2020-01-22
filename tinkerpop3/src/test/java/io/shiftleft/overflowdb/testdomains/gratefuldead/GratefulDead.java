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

  public static OdbGraph newGraphWithData() throws IOException {
    OdbGraph graph = newGraph();
    loadData(graph);
    return graph;
  }

  public static OdbGraph newGraphWithData(String path) throws IOException {
    OdbGraph graph = newGraph(OdbConfig.withDefaults().withStorageLocation(path));
    loadData(graph);
    return graph;
  }


  public static void loadData(OdbGraph graph) throws IOException {
    graph.io(IoCore.graphml()).readGraph("../src/test/resources/grateful-dead.xml");
  }


}
