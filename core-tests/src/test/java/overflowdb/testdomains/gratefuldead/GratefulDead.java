package overflowdb.testdomains.gratefuldead;

import overflowdb.Config;
import overflowdb.Graph;
import overflowdb.formats.GraphML;

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

  public static Graph openAndLoadSampleData() {
    Graph graph = GratefulDead.newGraph();
    loadData(graph);
    return graph;
  }

  public static Graph openAndLoadSampleData(String path) {
    Graph graph = GratefulDead.newGraph(Config.withDefaults().withStorageLocation(path));
    loadData(graph);
    return graph;
  }


  public static void loadData(Graph graph) {
    GraphML.insert("src/test/resources/grateful-dead.xml", graph);
  }

}
