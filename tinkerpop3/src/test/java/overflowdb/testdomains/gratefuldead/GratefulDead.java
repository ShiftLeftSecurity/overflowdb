package overflowdb.testdomains.gratefuldead;

import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import overflowdb.OdbConfig;
import overflowdb.OdbGraph;
import overflowdb.OdbGraphTp3;

import java.io.IOException;
import java.util.Arrays;

public class GratefulDead {
  public static OdbGraphTp3 open() {
    return open(OdbConfig.withoutOverflow());
  }

  public static OdbGraphTp3 open(OdbConfig config) {
    return OdbGraphTp3.wrap(
        OdbGraph.open(
          config,
          Arrays.asList(Song.factory, Artist.factory),
          Arrays.asList(FollowedBy.factory, SungBy.factory, WrittenBy.factory)
        )
    );
  }

  public static OdbGraphTp3 openAndLoadSampleData() throws IOException {
    OdbGraphTp3 graph = open();
    loadData(graph);
    return graph;
  }

  public static OdbGraphTp3 openAndLoadSampleData(String path) throws IOException {
    OdbGraphTp3 graph = open(OdbConfig.withDefaults().withStorageLocation(path));
    loadData(graph);
    return graph;
  }


  public static void loadData(OdbGraphTp3 graph) throws IOException {
    graph.io(IoCore.graphml()).readGraph("../src/test/resources/grateful-dead.xml");
  }


}
