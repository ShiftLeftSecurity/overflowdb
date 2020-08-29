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
  public void serializeVertex() throws IOException {
    try (Graph graph = SimpleDomain.newGraph()) {
      NodeSerializer serializer = new NodeSerializer(false);
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
      assertEquals(testNode.id(), deserializedRef.id);
      assertEquals(TestNode.LABEL, deserializedRef.label());
    }
  }

  @Test
  public void serializeWithEdge() throws IOException {
    try (Graph graph = SimpleDomain.newGraph()) {
      NodeSerializer serializer = new NodeSerializer(true);
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

  private NodeDeserializer newDeserializer(Graph graph) {
    Map<Integer, NodeFactory> nodeFactories = new HashMap();
    nodeFactories.put(TestNodeDb.layoutInformation.labelId, TestNode.factory);
    return new NodeDeserializer(graph, nodeFactories, true);
  }

}
