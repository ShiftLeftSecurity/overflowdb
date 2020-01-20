package io.shiftleft.overflowdb.storage;

import io.shiftleft.overflowdb.NodeFactory;
import io.shiftleft.overflowdb.NodeRef;
import io.shiftleft.overflowdb.OdbGraph;
import io.shiftleft.overflowdb.testdomains.simple.TestEdge;
import io.shiftleft.overflowdb.testdomains.simple.TestNode;
import io.shiftleft.overflowdb.testdomains.simple.SimpleDomain;
import io.shiftleft.overflowdb.testdomains.simple.TestNodeDb;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SerializerTest {

  @Test
  public void serializeVertex() throws IOException {
    try (OdbGraph graph = SimpleDomain.newGraph()) {
      NodeSerializer serializer = new NodeSerializer();
      NodeDeserializer deserializer = newDeserializer(graph);
      TestNode testNode = (TestNode) graph.addVertex(
          T.label, TestNode.LABEL,
          TestNode.STRING_PROPERTY, "StringValue",
          TestNode.INT_PROPERTY, 42,
          TestNode.STRING_LIST_PROPERTY, Arrays.asList("stringOne", "stringTwo"),
          TestNode.INT_LIST_PROPERTY, Arrays.asList(42, 43)
      );

      TestNodeDb testNodeDb = testNode.get();
      byte[] bytes = serializer.serialize(testNodeDb);
      Vertex deserialized = deserializer.deserialize(bytes);

      assertEquals(testNodeDb.id(), deserialized.id());
      assertEquals(testNodeDb.label(), deserialized.label());
      assertEquals(testNodeDb.valueMap(), ((TestNodeDb) deserialized).valueMap());

      final NodeRef deserializedRef = deserializer.deserializeRef(bytes);
      assertEquals(testNode.id(), deserializedRef.id);
      assertEquals(TestNode.LABEL, deserializedRef.label());
    }
  }

  @Test
  public void serializeWithEdge() throws IOException {
    try (OdbGraph graph = SimpleDomain.newGraph()) {
      NodeSerializer serializer = new NodeSerializer();
      NodeDeserializer deserializer = newDeserializer(graph);

      TestNode testNode1 = (TestNode) graph.addVertex(T.label, TestNode.LABEL);
      TestNode testNode2 = (TestNode) graph.addVertex(T.label, TestNode.LABEL);
      TestEdge testEdge = (TestEdge) testNode1.addEdge(TestEdge.LABEL, testNode2, TestEdge.LONG_PROPERTY, Long.MAX_VALUE);

      TestNodeDb testNode1Db = testNode1.get();
      TestNodeDb testNode2Db = testNode2.get();
      Vertex v0Deserialized = deserializer.deserialize(serializer.serialize(testNode1Db));
      Vertex v1Deserialized = deserializer.deserialize(serializer.serialize(testNode2Db));

      Edge edgeViaV0Deserialized = v0Deserialized.edges(Direction.OUT, TestEdge.LABEL).next();
      Edge edgeViaV1Deserialized = v1Deserialized.edges(Direction.IN, TestEdge.LABEL).next();

      assertEquals(testEdge.id(), edgeViaV0Deserialized.id());
      assertEquals(testEdge.id(), edgeViaV1Deserialized.id());
      assertEquals(TestEdge.LABEL, edgeViaV0Deserialized.label());
      assertEquals(TestEdge.LABEL, edgeViaV1Deserialized.label());
      assertEquals(Long.MAX_VALUE, (long) edgeViaV0Deserialized.value(TestEdge.LONG_PROPERTY));
      assertEquals(Long.MAX_VALUE, (long) edgeViaV1Deserialized.value(TestEdge.LONG_PROPERTY));

      assertEquals(testNode1.id(), edgeViaV0Deserialized.outVertex().id());
      assertEquals(testNode2.id(), edgeViaV0Deserialized.inVertex().id());
      assertEquals(testNode1.id(), edgeViaV1Deserialized.outVertex().id());
      assertEquals(testNode2.id(), edgeViaV1Deserialized.inVertex().id());
    }
  }

  private NodeDeserializer newDeserializer(OdbGraph graph) {
    Map<Integer, NodeFactory> vertexFactories = new HashMap();
    vertexFactories.put(TestNodeDb.layoutInformation.labelId, TestNode.factory);
    return new NodeDeserializer(graph, vertexFactories);
  }

}
