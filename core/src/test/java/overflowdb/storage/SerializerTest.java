package overflowdb.storage;

import org.junit.Test;
import overflowdb.Config;
import overflowdb.Node;
import overflowdb.NodeRef;
import overflowdb.Edge;
import overflowdb.Graph;
import overflowdb.testdomains.simple.FunkyList;
import overflowdb.testdomains.simple.SimpleDomain;
import overflowdb.testdomains.simple.TestEdge;
import overflowdb.testdomains.simple.TestNode;
import overflowdb.testdomains.simple.TestNodeDb;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class SerializerTest {

  @Test
  public void serializeNode() throws IOException {
    try (Graph graph = SimpleDomain.newGraph()) {
      NodeSerializer serializer = new NodeSerializer(false, graph.getStorage());
      NodeDeserializer deserializer = newDeserializer(graph);
      TestNode testNode = (TestNode) graph.addNode(
          TestNode.LABEL,
          TestNode.STRING_PROPERTY, "StringValue",
          TestNode.INT_PROPERTY, 42,
          TestNode.STRING_LIST_PROPERTY, Arrays.asList("stringOne", "stringTwo"),
          TestNode.INT_LIST_PROPERTY, Arrays.asList(42, 43)
      );

      TestNodeDb testNodeDb = testNode.get();
      byte[] bytes = serializer.serialize(testNodeDb);
      TestNodeDb deserialized = (TestNodeDb) deserializer.deserialize(bytes);

      assertEquals(testNodeDb.id(), deserialized.id());
      assertEquals(testNodeDb.label(), deserialized.label());
      assertEquals(testNodeDb.stringProperty(), deserialized.stringProperty());
      assertEquals(testNodeDb.intProperty(), deserialized.intProperty());
      assertEquals(testNodeDb.stringListProperty(), deserialized.stringListProperty());
      assertEquals(testNodeDb.intListProperty(), deserialized.intListProperty());

      final Map<String, Object> propertiesMap = testNodeDb.propertiesMap();
      final Map<String, Object> propertiesMapDeserialized = deserialized.propertiesMap();
      assertEquals(propertiesMap.get(TestNode.STRING_PROPERTY), propertiesMapDeserialized.get(TestNode.STRING_PROPERTY));
      assertEquals(propertiesMap.get(TestNode.INT_PROPERTY), propertiesMapDeserialized.get(TestNode.INT_PROPERTY));
      assertEquals(propertiesMap.get(TestNode.STRING_LIST_PROPERTY), propertiesMapDeserialized.get(TestNode.STRING_LIST_PROPERTY));
      assertArrayEquals((int[]) propertiesMap.get(TestNode.INT_LIST_PROPERTY),
                        (int[]) propertiesMapDeserialized.get(TestNode.INT_LIST_PROPERTY));

      final NodeRef deserializedRef = deserializer.deserializeRef(bytes);
      assertEquals(testNode.id(), deserializedRef.id());
      assertEquals(TestNode.LABEL, deserializedRef.label());
    }
  }

  @Test
  public void convertCustomTypes() throws IOException {
    try (Graph graph = SimpleDomain.newGraph()) {
      Function<Object, Object> convertPropertyForPersistence = value -> {
       if (value instanceof FunkyList) return FunkyList.toStorageType.apply((FunkyList) value);
       else return value;
      };
      NodeSerializer serializer = new NodeSerializer(false, graph.getStorage(), convertPropertyForPersistence);
      NodeDeserializer deserializer = newDeserializer(graph);
      TestNode testNode = (TestNode) graph.addNode(
          TestNode.LABEL,
          TestNode.FUNKY_LIST_PROPERTY, new FunkyList().add("anthropomorphic").add("boondoggle")
      );

      TestNodeDb testNodeDb = testNode.get();
      byte[] bytes = serializer.serialize(testNodeDb);
      Node deserialized = deserializer.deserialize(bytes);

      assertEquals(testNodeDb.id(), deserialized.id());
      assertEquals(testNodeDb.propertiesMap(), deserialized.propertiesMap());
      TestNodeDb deserializedTestNode = (TestNodeDb) deserialized;
      assertEquals(deserializedTestNode.funkyList().getEntries(), Arrays.asList("anthropomorphic", "boondoggle"));
    }
  }

  @Test
  public void serializeWithEdge() throws IOException {
    try (Graph graph = SimpleDomain.newGraph()) {
      NodeSerializer serializer = new NodeSerializer(true, graph.getStorage());
      NodeDeserializer deserializer = newDeserializer(graph);

      TestNode testNode1 = (TestNode) graph.addNode(TestNode.LABEL);
      TestNode testNode2 = (TestNode) graph.addNode(TestNode.LABEL);
      testNode1.addEdge(TestEdge.LABEL, testNode2, TestEdge.LONG_PROPERTY, Long.MAX_VALUE);

      TestNodeDb testNode1Db = testNode1.get();
      TestNodeDb testNode2Db = testNode2.get();
      Node n0Deserialized = deserializer.deserialize(serializer.serialize(testNode1Db));
      Node n1Deserialized = deserializer.deserialize(serializer.serialize(testNode2Db));

      Edge edgeViaN0Deserialized = n0Deserialized.outE(TestEdge.LABEL).next();
      Edge edgeViaN1Deserialized = n1Deserialized.inE(TestEdge.LABEL).next();

      assertEquals(TestEdge.LABEL, edgeViaN0Deserialized.label());
      assertEquals(TestEdge.LABEL, edgeViaN1Deserialized.label());
      assertEquals(Long.MAX_VALUE, (long) edgeViaN0Deserialized.property(TestEdge.LONG_PROPERTY));
      assertEquals(Long.MAX_VALUE, (long) edgeViaN1Deserialized.property(TestEdge.LONG_PROPERTY));

      assertEquals(testNode1, edgeViaN0Deserialized.outNode());
      assertEquals(testNode2, edgeViaN0Deserialized.inNode());
      assertEquals(testNode1, edgeViaN1Deserialized.outNode());
      assertEquals(testNode2, edgeViaN1Deserialized.inNode());
    }
  }

  @Test
  public void serializeWithDefaultPropertyValues() throws IOException {
    File storageLocation = File.createTempFile("overflowdb-test", "bin");
    storageLocation.deleteOnExit();

    Graph graph = Graph.open(
        Config.withoutOverflow().withStorageLocation(storageLocation.getPath()),
        Arrays.asList(overflowdb.testdomains.configurabledefaults.TestNode.factory("DEFAULT_STRING_VALUE")),
        Arrays.asList(overflowdb.testdomains.configurabledefaults.TestEdge.factory(-99))
    );

    overflowdb.testdomains.configurabledefaults.TestNode testNode1 =
        (overflowdb.testdomains.configurabledefaults.TestNode) graph.addNode(overflowdb.testdomains.configurabledefaults.TestNode.LABEL);
    overflowdb.testdomains.configurabledefaults.TestNode testNode2 =
        (overflowdb.testdomains.configurabledefaults.TestNode) graph.addNode(overflowdb.testdomains.configurabledefaults.TestNode.LABEL);
    overflowdb.testdomains.configurabledefaults.TestEdge testEdge =
        (overflowdb.testdomains.configurabledefaults.TestEdge) testNode1.addEdge(TestEdge.LABEL, testNode2);

    // properties are set to default values
    final String stringPropertyKey = overflowdb.testdomains.configurabledefaults.TestNode.STRING_PROPERTY;
    final String longPropertyKey = overflowdb.testdomains.configurabledefaults.TestEdge.LONG_PROPERTY;
    assertEquals("DEFAULT_STRING_VALUE", testNode1.stringProperty());
    assertEquals("DEFAULT_STRING_VALUE", testNode1.property(stringPropertyKey));
    assertEquals("DEFAULT_STRING_VALUE", testNode1.propertiesMap().get(stringPropertyKey));
    assertFalse(testNode1.get().propertiesMapForStorage().containsKey(stringPropertyKey));
    assertEquals(Long.valueOf(-99l), testEdge.longProperty());
    assertEquals(-99l, testEdge.property(longPropertyKey));
    assertEquals(-99l, testEdge.propertiesMap().get(longPropertyKey));
    graph.close();

    // to verify that default property values are not serialized, we're reopening the graph with different `default value` settings
    graph = Graph.open(
        Config.withoutOverflow().withStorageLocation(storageLocation.getPath()),
        Arrays.asList(overflowdb.testdomains.configurabledefaults.TestNode.factory("NEW_DEFAULT_STRING_VALUE")),
        Arrays.asList(overflowdb.testdomains.configurabledefaults.TestEdge.factory(-49))
    );
    overflowdb.testdomains.configurabledefaults.TestNode n1Deserialized =
        (overflowdb.testdomains.configurabledefaults.TestNode) graph.nodes().next();
    overflowdb.testdomains.configurabledefaults.TestEdge edge1Deserialized =
        (overflowdb.testdomains.configurabledefaults.TestEdge) n1Deserialized.bothE(TestEdge.LABEL).next();
    assertEquals("NEW_DEFAULT_STRING_VALUE", n1Deserialized.stringProperty());
    assertEquals("NEW_DEFAULT_STRING_VALUE", n1Deserialized.property(stringPropertyKey));
    assertEquals("NEW_DEFAULT_STRING_VALUE", n1Deserialized.propertiesMap().get(stringPropertyKey));
    assertFalse(n1Deserialized.get().propertiesMapForStorage().containsKey(stringPropertyKey));
    assertEquals(Long.valueOf(-49l), edge1Deserialized.longProperty());
    assertEquals(-49l, edge1Deserialized.property(longPropertyKey));
    assertEquals(-49l, edge1Deserialized.propertiesMap().get(longPropertyKey));
  }

  private NodeDeserializer newDeserializer(Graph graph) {
    return new NodeDeserializer(
        graph,
        new HashMap() {{ put(TestNodeDb.layoutInformation.label, TestNode.factory); }},
        true,
        graph.getStorage());
  }

}
