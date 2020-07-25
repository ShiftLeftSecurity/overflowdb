package overflowdb.testdomains.gratefuldead;

import overflowdb.OdbConfig;
import overflowdb.OdbGraph;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;

import java.io.IOException;
import java.util.Arrays;

public class GratefulDead {
  public static OdbGraph open() {
    return open(OdbConfig.withoutOverflow());
  }

  public static OdbGraph open(OdbConfig config) {
    return OdbGraph.open(
        config,
        Arrays.asList(Song.factory, Artist.factory),
        Arrays.asList(FollowedBy.factory, SungBy.factory, WrittenBy.factory)
    );
  }

  public static OdbGraph openAndLoadSampleData() throws IOException {
    OdbGraph graph = open();
    loadData(graph);
    return graph;
  }

  public static OdbGraph openAndLoadSampleData(String path) throws IOException {
    OdbGraph graph = open(OdbConfig.withDefaults().withStorageLocation(path));
    loadData(graph);
    return graph;
  }


  public static void loadData(OdbGraph graph) throws IOException {
    graph.io(IoCore.graphml()).readGraph("../src/test/resources/grateful-dead.xml");
  }


}
