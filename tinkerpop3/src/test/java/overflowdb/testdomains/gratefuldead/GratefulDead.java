package overflowdb.testdomains.gratefuldead;

import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import overflowdb.Config;
import overflowdb.Graph;
import overflowdb.tinkerpop.OdbGraphTp3;

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

  public static OdbGraphTp3 openAndLoadSampleData() throws IOException {
    OdbGraphTp3 graph = OdbGraphTp3.wrap(open());
    loadData(graph);
    return graph;
  }

  public static OdbGraphTp3 openAndLoadSampleData(String path) throws IOException {
    OdbGraphTp3 graph =
      OdbGraphTp3.wrap(
        open(Config.withDefaults().withStorageLocation(path))
      );
    loadData(graph);
    return graph;
  }


  public static void loadData(OdbGraphTp3 graph) throws IOException {
    graph.io(IoCore.graphml()).readGraph("../src/test/resources/grateful-dead.xml");
  }


}
