package overflowdb.storage;

import org.junit.Test;
import overflowdb.Config;
import overflowdb.Edge;
import overflowdb.EdgeFactory;
import overflowdb.EdgeLayoutInformation;
import overflowdb.Graph;
import overflowdb.Node;
import overflowdb.NodeDb;
import overflowdb.NodeFactory;
import overflowdb.NodeLayoutInformation;
import overflowdb.NodeRef;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class BackwardsCompatibilityTest {

  @Test
  public void shouldLoadOldStorageFormatWhenAddingEdgeType() throws IOException {
    final File storageFile = Files.createTempFile("overflowdb", "bin").toFile();
    storageFile.delete();
    Config config = Config.withDefaults().withStorageLocation(storageFile.getAbsolutePath());

    {
      Graph graph = Graph.open(config,
          Arrays.asList(SchemaV1.Thing1.nodeFactory, SchemaV1.Thing2.nodeFactory),
          Arrays.asList(SchemaV1.Connection1.factory));
      Node thing1 = graph.addNode(SchemaV1.Thing1.LABEL);
      Node thing2 = graph.addNode(SchemaV1.Thing2.LABEL);
      thing1.addEdge(SchemaV1.Connection1.LABEL, thing2);
      assertEquals(2, graph.nodeCount());
      assertEquals(1, graph.edgeCount());
      graph.close();
    }

//    {
//      Graph graph = Graph.open(config,
//          Arrays.asList(SchemaV2.Thing.nodeFactory),
////          Arrays.asList(SchemaV1.Thing.nodeFactory),
//          Arrays.asList(SchemaV1.Connection1.factory, SchemaV2.Connection2.factory));
//      assertEquals(2, graph.nodeCount());
//
////      final Iterator<Node> nodes = graph.nodes();
////      while (nodes.hasNext()) {
////        nodes.next().outE().forEachRemaining(System.out::println);
//        // why the heck is there a Connection2? only when using SchemaV2.Thing.nodeFactory
//        // verify: is this fixed by the backwards compat handling?
//        // TODO: try both ways: (Connection2, Connection) and (Connection, Connection2) in layoutInformation
////      }
//      Node node0 = graph.node(0);
//      node0.outE().forEachRemaining(System.out::println);
//
//      Node node1 = graph.node(1);
//      node1.outE().forEachRemaining(System.out::println);
//
////      graph.edges().forEachRemaining(e -> System.out.println(e));
//      assertEquals(1, graph.edgeCount());
//      // TODO more tests: walk edge, ensure it's of the right type
//      graph.close();
//    }

    storageFile.delete(); //cleanup after test
  }
}

class SchemaV1 {
  static class Thing1 extends DummyNodeDb {
    static final int LABEL_ID = 1;
    static final String LABEL = "Thing1";

    static NodeLayoutInformation layoutInformation = new NodeLayoutInformation(
        LABEL_ID,
        Collections.emptySet(),
        Arrays.asList(Connection1.layoutInformation),
        Arrays.asList()
    );

    static NodeFactory<Thing1> nodeFactory = new NodeFactory<Thing1>() {
      public String forLabel() {
        return LABEL;
      }

      public int forLabelId() {
        return LABEL_ID;
      }

      public Thing1 createNode(NodeRef<Thing1> ref) {
        return new Thing1(ref);
      }

      @Override
      public NodeRef<Thing1> createNodeRef(Graph graph, long id) {
        return new NodeRef<Thing1>(graph, id) {
          @Override
          public String label() {
            return LABEL;
          }
        };
      }

      @Override
      public NodeLayoutInformation layoutInformation() {
        return layoutInformation;
      }
    };

    protected Thing1(NodeRef ref) {
      super(ref);
    }

    @Override
    public NodeLayoutInformation layoutInformation() {
      return layoutInformation;
    }
  }

  static class Thing2 extends DummyNodeDb {
    static final int LABEL_ID = 2;
    static final String LABEL = "Thing2";

    static NodeLayoutInformation layoutInformation = new NodeLayoutInformation(
        LABEL_ID,
        Collections.emptySet(),
        Arrays.asList(),
        Arrays.asList(Connection1.layoutInformation)
    );

    static NodeFactory<Thing2> nodeFactory = new NodeFactory<Thing2>() {
      public String forLabel() {
        return LABEL;
      }

      public int forLabelId() {
        return LABEL_ID;
      }

      public Thing2 createNode(NodeRef<Thing2> ref) {
        return new Thing2(ref);
      }

      @Override
      public NodeRef<Thing2> createNodeRef(Graph graph, long id) {
        return new NodeRef<Thing2>(graph, id) {
          @Override
          public String label() {
            return LABEL;
          }
        };
      }

      @Override
      public NodeLayoutInformation layoutInformation() {
        return layoutInformation;
      }
    };

    protected Thing2(NodeRef ref) {
      super(ref);
    }

    @Override
    public NodeLayoutInformation layoutInformation() {
      return layoutInformation;
    }
  }

  static class Connection1 extends Edge {
    public static final String LABEL = "Connection1";
    public static EdgeLayoutInformation layoutInformation = new EdgeLayoutInformation(LABEL, Collections.EMPTY_SET);

    public static EdgeFactory<Connection1> factory = new EdgeFactory<Connection1>() {
      @Override
      public String forLabel() {
        return LABEL;
      }

      @Override
      public Connection1 createEdge(Graph graph, NodeRef<NodeDb> outNode, NodeRef<NodeDb> inNode) {
        return new Connection1(graph, outNode, inNode, Collections.EMPTY_SET);
      }
    };

    public Connection1(Graph graph, NodeRef outNode, NodeRef inVertex, Set<String> specificKeys) {
      super(graph, LABEL, outNode, inVertex, specificKeys);
    }
  }
}

/* additions compared to SchemaV1: there's an additional edge 'Connection2' between Thing1 and Thing2 */
class SchemaV2 {

  static class Connection2 extends Edge {
    public static final String LABEL = "Connection2";
    public static EdgeLayoutInformation layoutInformation = new EdgeLayoutInformation(LABEL, Collections.EMPTY_SET);

    public static EdgeFactory<Connection2> factory = new EdgeFactory<Connection2>() {
      @Override
      public String forLabel() {
        return LABEL;
      }

      @Override
      public Connection2 createEdge(Graph graph, NodeRef<NodeDb> outNode, NodeRef<NodeDb> inNode) {
        return new Connection2(graph, outNode, inNode, Collections.EMPTY_SET);
      }
    };

    public Connection2(Graph graph, NodeRef outNode, NodeRef inVertex, Set<String> specificKeys) {
      super(graph, LABEL, outNode, inVertex, specificKeys);
    }
  }
}

abstract class DummyNodeDb extends NodeDb {
  protected DummyNodeDb(NodeRef ref) {
    super(ref);
  }

  public Map<String, Object> valueMap() {
    return Collections.EMPTY_MAP;
  }

  protected void updateSpecificProperty(String key, Object value) {}

  protected void removeSpecificProperty(String key) {}

  public Object property(String key) {
    return null;
  }
}