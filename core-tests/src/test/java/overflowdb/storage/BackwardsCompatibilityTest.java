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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class BackwardsCompatibilityTest {

  /** With a domain (SchemaV1) with two node types (Thing1/2) and one edge type (Connection1, which has one property),
   * create a sample graph and persist it.
   *
   * Then, reopen the file after some additions have been made to the schema (SchemaV2):
   * now there's another edge type (Connection2), and Connection1 has one additional property.
   *
   * Verify that we're still able to load the graph, since there's only additions to the schema.
   *  */
  @Test
  public void shouldLoadOldStorageFormatWhenAddingEdgeType() throws IOException {
    final File storageFile = Files.createTempFile("overflowdb", "bin").toFile();
    Config config = Config.withDefaults().withStorageLocation(storageFile.getAbsolutePath());

    final long thing1Id;
    {
      Graph graph = SchemaV1.newEmptyGraph(config);
      Node thing1 = graph.addNode(SchemaV1.Thing1.LABEL);
      Node thing2 = graph.addNode(SchemaV1.Thing2.LABEL);
      thing1.addEdge(SchemaV1.Connection1.LABEL, thing2, SchemaV1.Connection1.NAME, "thing 1");
      assertEquals(2, graph.nodeCount());
      assertEquals(1, graph.edgeCount());
      thing1Id = thing1.id();
      graph.close();
    }

    {
      Graph graph = SchemaV2.newEmptyGraph(config);
      Node thing1 = graph.node(thing1Id);
      SchemaV2.Thing1 thing1Typed = ((NodeRef<SchemaV2.Thing1>) thing1).get();
      SchemaV2.Connection1 connection = (SchemaV2.Connection1) thing1.outE().next();
      assertEquals(1, connection.propertiesMap().size());
      Node thing2 = connection.inNode();
      connection = (SchemaV2.Connection1) thing2.inE().next();
      assertEquals(1, connection.propertiesMap().size());
      SchemaV2.Thing2 thing2Typed = ((NodeRef<SchemaV2.Thing2>) thing2).get();

      assertEquals(2, graph.nodeCount());
      assertEquals(1, graph.edgeCount());

      graph.close();
    }

    storageFile.delete(); //cleanup after test
  }

  @Test(expected = RuntimeException.class)
  /** Same as above, but this time we persist a graph based on SchemaV2, and try to open it from SchemaV1.
   * Since SchemaV1 doesn't know about Connection2 and the additional Connection1 property, this should fail.
   *  */
  public void shouldThrowExceptionForUnsupportedProperty() throws IOException {
    final File storageFile = Files.createTempFile("overflowdb", "bin").toFile();
    Config config = Config.withDefaults().withStorageLocation(storageFile.getAbsolutePath());

    {
      Graph graph = SchemaV2.newEmptyGraph(config);
      Node thing1 = graph.addNode(SchemaV1.Thing1.LABEL);
      Node thing2 = graph.addNode(SchemaV1.Thing2.LABEL);
      thing1.addEdge(SchemaV2.Connection1.LABEL, thing2, SchemaV2.Connection1.ADDITIONAL_PROP, "additional property");
      assertEquals(2, graph.nodeCount());
      assertEquals(1, graph.edgeCount());
      graph.close();
    }

    {
      Graph graph = SchemaV1.newEmptyGraph(config);
      assertEquals(2, graph.nodeCount());
      assertEquals(1, graph.edgeCount());

      graph.close();
    }

    storageFile.delete(); //cleanup after test
  }

  @Test(expected = RuntimeException.class)
  /** Same as above, but this time we persist a graph based on SchemaV2, and try to open it from SchemaV1.
   * Since SchemaV1 doesn't know about Connection2 and the additional Connection1 property, this should fail.
   *  */
  public void shouldThrowExceptionForUnsupportedEdge() throws IOException {
    final File storageFile = Files.createTempFile("overflowdb", "bin").toFile();
    Config config = Config.withDefaults().withStorageLocation(storageFile.getAbsolutePath());

    {
      Graph graph = SchemaV2.newEmptyGraph(config);
      Node thing1 = graph.addNode(SchemaV1.Thing1.LABEL);
      Node thing2 = graph.addNode(SchemaV1.Thing2.LABEL);
      thing1.addEdge(SchemaV2.Connection2.LABEL, thing2); // TODO additional test
      assertEquals(2, graph.nodeCount());
      assertEquals(1, graph.edgeCount());
      graph.close();
    }

    {
      Graph graph = SchemaV1.newEmptyGraph(config);
      assertEquals(2, graph.nodeCount());
      assertEquals(1, graph.edgeCount());

      graph.close();
    }

    storageFile.delete(); //cleanup after test
  }
}

class SchemaV1 {

  public static Graph newEmptyGraph(Config config) {
    return Graph.open(config,
        Arrays.asList(SchemaV1.Thing1.nodeFactory, SchemaV1.Thing2.nodeFactory),
        Arrays.asList(SchemaV1.Connection1.factory));
  }

  static class Thing1 extends DummyNodeDb {
    static final String LABEL = "Thing1";

    static NodeLayoutInformation layoutInformation = new NodeLayoutInformation(
        LABEL,
        Collections.emptySet(),
        Arrays.asList(Connection1.layoutInformation),
        Arrays.asList()
    );

    static NodeFactory<Thing1> nodeFactory = new NodeFactory<Thing1>() {
      public String forLabel() {
        return LABEL;
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
    static final String LABEL = "Thing2";

    static NodeLayoutInformation layoutInformation = new NodeLayoutInformation(
        LABEL,
        Collections.emptySet(),
        Arrays.asList(),
        Arrays.asList(Connection1.layoutInformation)
    );

    static NodeFactory<Thing2> nodeFactory = new NodeFactory<Thing2>() {
      public String forLabel() {
        return LABEL;
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
    public static final String NAME = "name";
    private static final Set<String> propertyKeys = new HashSet<>(Arrays.asList(NAME));

    public static EdgeLayoutInformation layoutInformation = new EdgeLayoutInformation(LABEL, propertyKeys);

    public static EdgeFactory<Connection1> factory = new EdgeFactory<Connection1>() {
      @Override
      public String forLabel() {
        return LABEL;
      }

      @Override
      public Connection1 createEdge(Graph graph, NodeRef<NodeDb> outNode, NodeRef<NodeDb> inNode) {
        return new Connection1(graph, outNode, inNode);
      }
    };

    public Connection1(Graph graph, NodeRef outNode, NodeRef inVertex) {
      super(graph, LABEL, outNode, inVertex, propertyKeys);
    }
  }
}

/* additions compared to SchemaV1: there's an additional edge 'Connection2' between Thing1 and Thing2 */
class SchemaV2 {

  public static Graph newEmptyGraph(Config config) {
    return Graph.open(config,
        Arrays.asList(SchemaV2.Thing1.nodeFactory, SchemaV2.Thing2.nodeFactory),
        Arrays.asList(SchemaV2.Connection1.factory, SchemaV2.Connection2.factory));
  }

  static class Thing1 extends DummyNodeDb {
    static final String LABEL = "Thing1";

    static NodeLayoutInformation layoutInformation = new NodeLayoutInformation(
        LABEL,
        Collections.emptySet(),
        Arrays.asList(Connection2.layoutInformation, Connection1.layoutInformation),
        Arrays.asList()
    );

    static NodeFactory<Thing1> nodeFactory = new NodeFactory<Thing1>() {
      public String forLabel() {
        return LABEL;
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
    static final String LABEL = "Thing2";

    static NodeLayoutInformation layoutInformation = new NodeLayoutInformation(
        LABEL,
        Collections.emptySet(),
        Arrays.asList(),
        Arrays.asList(Connection2.layoutInformation, Connection1.layoutInformation)
    );

    static NodeFactory<Thing2> nodeFactory = new NodeFactory<Thing2>() {
      public String forLabel() {
        return LABEL;
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
    };

    protected Thing2(NodeRef ref) {
      super(ref);
    }

    @Override
    public NodeLayoutInformation layoutInformation() {
      return layoutInformation;
    }
  }

  // just like SchemaV1.Connection1, but one more property
  static class Connection1 extends Edge {
    public static final String LABEL = "Connection1";
    public static final String NAME = "name";
    public static final String ADDITIONAL_PROP = "foo";
    private static final Set<String> propertyKeys = new HashSet<>(Arrays.asList(NAME, ADDITIONAL_PROP));

    public static EdgeLayoutInformation layoutInformation = new EdgeLayoutInformation(LABEL, propertyKeys);

    public static EdgeFactory<Connection1> factory = new EdgeFactory<Connection1>() {
      @Override
      public String forLabel() {
        return LABEL;
      }

      @Override
      public Connection1 createEdge(Graph graph, NodeRef<NodeDb> outNode, NodeRef<NodeDb> inNode) {
        return new Connection1(graph, outNode, inNode);
      }
    };

    public Connection1(Graph graph, NodeRef outNode, NodeRef inVertex) {
      super(graph, LABEL, outNode, inVertex, propertyKeys);
    }
  }

  static class Connection2 extends Edge {
    public static final String LABEL = "Connection2";
    public static final String BAR = "bar";
    private static final Set<String> propertyKeys = new HashSet<>(Arrays.asList(BAR));
    public static EdgeLayoutInformation layoutInformation = new EdgeLayoutInformation(LABEL, propertyKeys);

    public static EdgeFactory<Connection2> factory = new EdgeFactory<Connection2>() {
      @Override
      public String forLabel() {
        return LABEL;
      }

      @Override
      public Connection2 createEdge(Graph graph, NodeRef<NodeDb> outNode, NodeRef<NodeDb> inNode) {
        return new Connection2(graph, outNode, inNode);
      }
    };

    public Connection2(Graph graph, NodeRef outNode, NodeRef inVertex) {
      super(graph, LABEL, outNode, inVertex, propertyKeys);
    }
  }
}

abstract class DummyNodeDb extends NodeDb {
  protected DummyNodeDb(NodeRef ref) {
    super(ref);
  }

  protected void updateSpecificProperty(String key, Object value) {}

  protected void removeSpecificProperty(String key) {}

  public Object property(String key) {
    return null;
  }
}
