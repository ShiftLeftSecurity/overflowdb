package overflowdb.testdomains.gratefuldead;

import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import overflowdb.Config;
import overflowdb.Graph;
import overflowdb.tinkerpop.OdbGraphTp3;

import java.io.IOException;
import java.util.Arrays;

public class GratefulDeadTp3 {

  public static OdbGraphTp3 openAndLoadSampleData() throws IOException {
    OdbGraphTp3 graph = OdbGraphTp3.wrap(GratefulDead.open());
    loadData(graph);
    return graph;
  }

  public static OdbGraphTp3 openAndLoadSampleData(String path) throws IOException {
    OdbGraphTp3 graph =
      OdbGraphTp3.wrap(
        GratefulDead.open(Config.withDefaults().withStorageLocation(path))
      );
    loadData(graph);
    return graph;
  }


  public static void loadData(OdbGraphTp3 graph) throws IOException {
    graph.io(IoCore.graphml()).readGraph("../src/test/resources/grateful-dead.xml");
  }


}
