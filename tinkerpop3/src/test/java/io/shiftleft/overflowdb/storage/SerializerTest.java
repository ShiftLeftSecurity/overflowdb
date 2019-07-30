package io.shiftleft.overflowdb.storage;

import io.shiftleft.overflowdb.structure.NodeRef;
import io.shiftleft.overflowdb.structure.OverflowDbGraph;
import io.shiftleft.overflowdb.structure.OverflowDbTestEdge;
import io.shiftleft.overflowdb.structure.OverflowDbTestNode;
import io.shiftleft.overflowdb.structure.OverflowElementFactory;
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
    try (OverflowDbGraph graph = newGraph()) {
      NodeSerializer serializer = new NodeSerializer();
      NodeDeserializer deserializer = newDeserializer(graph);
      Vertex vertexRef = graph.addVertex(
          T.label, OverflowDbTestNode.label,
          OverflowDbTestNode.STRING_PROPERTY, "StringValue",
          OverflowDbTestNode.INT_PROPERTY, 42,
          OverflowDbTestNode.STRING_LIST_PROPERTY, Arrays.asList("stringOne", "stringTwo"),
          OverflowDbTestNode.INT_LIST_PROPERTY, Arrays.asList(42, 43)
      );

      OverflowDbTestNode underlyingVertexDb = ((NodeRef<OverflowDbTestNode>) vertexRef).get();
      byte[] bytes = serializer.serialize(underlyingVertexDb);
      Vertex deserialized = deserializer.deserialize(bytes);

      assertEquals(underlyingVertexDb.id(), deserialized.id());
      assertEquals(underlyingVertexDb.label(), deserialized.label());
      assertEquals(underlyingVertexDb.valueMap(), ((OverflowDbTestNode) deserialized).valueMap());

      final NodeRef deserializedRef = deserializer.deserializeRef(bytes);
      assertEquals(vertexRef.id(), deserializedRef.id);
      assertEquals(OverflowDbTestNode.label, deserializedRef.label());
    }
  }

  @Test
  public void serializeWithEdge() throws IOException {
    try (OverflowDbGraph graph = newGraph()) {
      NodeSerializer serializer = new NodeSerializer();
      NodeDeserializer deserializer = newDeserializer(graph);

      Vertex v0 = graph.addVertex(T.label, OverflowDbTestNode.label);
      Vertex v1 = graph.addVertex(T.label, OverflowDbTestNode.label);
      Edge edge = v0.addEdge(OverflowDbTestEdge.LABEL, v1, OverflowDbTestEdge.LONG_PROPERTY, Long.MAX_VALUE);

      OverflowDbTestNode v0Underlying = ((NodeRef<OverflowDbTestNode>) v0).get();
      OverflowDbTestNode v1Underlying = ((NodeRef<OverflowDbTestNode>) v1).get();
      Vertex v0Deserialized = deserializer.deserialize(serializer.serialize(v0Underlying));
      Vertex v1Deserialized = deserializer.deserialize(serializer.serialize(v1Underlying));

      Edge edgeViaV0Deserialized = v0Deserialized.edges(Direction.OUT, OverflowDbTestEdge.LABEL).next();
      Edge edgeViaV1Deserialized = v1Deserialized.edges(Direction.IN, OverflowDbTestEdge.LABEL).next();

      assertEquals(edge.id(), edgeViaV0Deserialized.id());
      assertEquals(edge.id(), edgeViaV1Deserialized.id());
      assertEquals(OverflowDbTestEdge.LABEL, edgeViaV0Deserialized.label());
      assertEquals(OverflowDbTestEdge.LABEL, edgeViaV1Deserialized.label());
      assertEquals(Long.MAX_VALUE, (long) edgeViaV0Deserialized.value(OverflowDbTestEdge.LONG_PROPERTY));
      assertEquals(Long.MAX_VALUE, (long) edgeViaV1Deserialized.value(OverflowDbTestEdge.LONG_PROPERTY));

      assertEquals(v0.id(), edgeViaV0Deserialized.outVertex().id());
      assertEquals(v1.id(), edgeViaV0Deserialized.inVertex().id());
      assertEquals(v0.id(), edgeViaV1Deserialized.outVertex().id());
      assertEquals(v1.id(), edgeViaV1Deserialized.inVertex().id());
    }
  }

  private NodeDeserializer newDeserializer(OverflowDbGraph graph) {
    Map<String, OverflowElementFactory.ForNode> vertexFactories = new HashMap();
    vertexFactories.put(OverflowDbTestNode.label, OverflowDbTestNode.factory);
    return new NodeDeserializer(graph, vertexFactories);
  }

  private OverflowDbGraph newGraph() {
    return OverflowDbGraph.open(
        Arrays.asList(OverflowDbTestNode.factory),
        Arrays.asList(OverflowDbTestEdge.factory)
    );
  }

}
