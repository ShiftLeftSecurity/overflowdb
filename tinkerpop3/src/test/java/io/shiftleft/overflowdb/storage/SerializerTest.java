package io.shiftleft.overflowdb.storage;

import io.shiftleft.overflowdb.NodeFactory;
import io.shiftleft.overflowdb.NodeRef;
import io.shiftleft.overflowdb.OdbGraph;
import io.shiftleft.overflowdb.testdomains.simple.TestEdge;
import io.shiftleft.overflowdb.testdomains.simple.TestNode;
import io.shiftleft.overflowdb.testdomains.simple.SimpleDomain;
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
      Vertex vertexRef = graph.addVertex(
          T.label, TestNode.LABEL,
          TestNode.STRING_PROPERTY, "StringValue",
          TestNode.INT_PROPERTY, 42,
          TestNode.STRING_LIST_PROPERTY, Arrays.asList("stringOne", "stringTwo"),
          TestNode.INT_LIST_PROPERTY, Arrays.asList(42, 43)
      );

      TestNode underlyingVertexDb = ((NodeRef<TestNode>) vertexRef).get();
      byte[] bytes = serializer.serialize(underlyingVertexDb);
      Vertex deserialized = deserializer.deserialize(bytes);

      assertEquals(underlyingVertexDb.id(), deserialized.id());
      assertEquals(underlyingVertexDb.label(), deserialized.label());
      assertEquals(underlyingVertexDb.valueMap(), ((TestNode) deserialized).valueMap());

      final NodeRef deserializedRef = deserializer.deserializeRef(bytes);
      assertEquals(vertexRef.id(), deserializedRef.id);
      assertEquals(TestNode.LABEL, deserializedRef.label());
    }
  }

  @Test
  public void serializeWithEdge() throws IOException {
    try (OdbGraph graph = SimpleDomain.newGraph()) {
      NodeSerializer serializer = new NodeSerializer();
      NodeDeserializer deserializer = newDeserializer(graph);

      Vertex v0 = graph.addVertex(T.label, TestNode.LABEL);
      Vertex v1 = graph.addVertex(T.label, TestNode.LABEL);
      Edge edge = v0.addEdge(TestEdge.LABEL, v1, TestEdge.LONG_PROPERTY, Long.MAX_VALUE);

      TestNode v0Underlying = ((NodeRef<TestNode>) v0).get();
      TestNode v1Underlying = ((NodeRef<TestNode>) v1).get();
      Vertex v0Deserialized = deserializer.deserialize(serializer.serialize(v0Underlying));
      Vertex v1Deserialized = deserializer.deserialize(serializer.serialize(v1Underlying));

      Edge edgeViaV0Deserialized = v0Deserialized.edges(Direction.OUT, TestEdge.LABEL).next();
      Edge edgeViaV1Deserialized = v1Deserialized.edges(Direction.IN, TestEdge.LABEL).next();

      assertEquals(edge.id(), edgeViaV0Deserialized.id());
      assertEquals(edge.id(), edgeViaV1Deserialized.id());
      assertEquals(TestEdge.LABEL, edgeViaV0Deserialized.label());
      assertEquals(TestEdge.LABEL, edgeViaV1Deserialized.label());
      assertEquals(Long.MAX_VALUE, (long) edgeViaV0Deserialized.value(TestEdge.LONG_PROPERTY));
      assertEquals(Long.MAX_VALUE, (long) edgeViaV1Deserialized.value(TestEdge.LONG_PROPERTY));

      assertEquals(v0.id(), edgeViaV0Deserialized.outVertex().id());
      assertEquals(v1.id(), edgeViaV0Deserialized.inVertex().id());
      assertEquals(v0.id(), edgeViaV1Deserialized.outVertex().id());
      assertEquals(v1.id(), edgeViaV1Deserialized.inVertex().id());
    }
  }

  private NodeDeserializer newDeserializer(OdbGraph graph) {
    Map<String, NodeFactory> vertexFactories = new HashMap();
    vertexFactories.put(TestNode.LABEL, TestNode.factory);
    return new NodeDeserializer(graph, vertexFactories);
  }

}
