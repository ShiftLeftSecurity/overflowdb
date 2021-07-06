package overflowdb.storage;

import org.junit.Test;
import overflowdb.Node;
import overflowdb.NodeFactory;
import overflowdb.NodeRef;
import overflowdb.Edge;
import overflowdb.Graph;
import overflowdb.testdomains.simple.SimpleDomain;
import overflowdb.testdomains.simple.TestEdge;
import overflowdb.testdomains.simple.TestNode;
import overflowdb.testdomains.simple.TestNodeDb;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

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
      Node deserialized = deserializer.deserialize(bytes);

      assertEquals(testNodeDb.id(), deserialized.id());
      assertEquals(testNodeDb.label(), deserialized.label());
      assertEquals(testNodeDb.propertyMap(), deserialized.propertyMap());

      final NodeRef deserializedRef = deserializer.deserializeRef(bytes);
      assertEquals(testNode.id(), deserializedRef.id());
      assertEquals(TestNode.LABEL, deserializedRef.label());
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
    try (Graph graph = SimpleDomain.newGraph()) {
      NodeSerializer serializer = new NodeSerializer(true, graph.getStorage());

      TestNode testNode1 = (TestNode) graph.addNode(TestNode.LABEL);
      TestNode testNode2 = (TestNode) graph.addNode(TestNode.LABEL);
      TestEdge testEdge = (TestEdge) testNode1.addEdge(TestEdge.LABEL, testNode2);

      // properties are set to default values
      assertEquals("DEFAULT_STRING_VALUE", testNode1.stringProperty());
      assertEquals("DEFAULT_STRING_VALUE", testNode1.property(TestNode.STRING_PROPERTY));
      assertEquals(new Long(-99l), testEdge.longProperty());
      assertEquals(new Long(-99l), testEdge.property(TestEdge.LONG_PROPERTY));

      // serialize the nodes, which implicitly also serializes the edge
      TestNodeDb testNode1Db = testNode1.get();
      TestNodeDb testNode2Db = testNode2.get();
      final byte[] n1Serialized = serializer.serialize(testNode1Db);
      final byte[] n2Serialized = serializer.serialize(testNode2Db);

      // to verify that default property values are not serialized, we're changing the implementation of Node/Edge to use different defaults
      Map<String, NodeFactory> nodeFactories = new HashMap();
      nodeFactories.put(TestNodeDb.layoutInformation.label, TestNode.factory);
      NodeDeserializer deserializer = new NodeDeserializer(graph, nodeFactories, true, graph.getStorage());
      TestNodeDb n1Deserialized = (TestNodeDb) deserializer.deserialize(n1Serialized);
      TestEdge edge1Deserialized = (TestEdge) n1Deserialized.outE(TestEdge.LABEL).next();
      assertEquals("NEW_DEFAULT_STRING_VALUE", n1Deserialized.stringProperty());
      assertEquals("NEW_DEFAULT_STRING_VALUE", n1Deserialized.property(TestNode.STRING_PROPERTY));
      assertEquals(new Long(-49l), edge1Deserialized.longProperty());
      assertEquals(new Long(-49l), edge1Deserialized.property(TestEdge.LONG_PROPERTY));
    }
  }

  private NodeDeserializer newDeserializer(Graph graph) {
    Map<String, NodeFactory> nodeFactories = new HashMap();
    nodeFactories.put(TestNodeDb.layoutInformation.label, TestNode.factory);
    return new NodeDeserializer(graph, nodeFactories, true, graph.getStorage());
  }

}
