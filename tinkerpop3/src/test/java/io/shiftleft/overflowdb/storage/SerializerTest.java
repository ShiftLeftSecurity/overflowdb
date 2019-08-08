package io.shiftleft.overflowdb.storage;

import io.shiftleft.overflowdb.structure.NodeFactory;
import io.shiftleft.overflowdb.structure.NodeRef;
import io.shiftleft.overflowdb.structure.OdbConfig;
import io.shiftleft.overflowdb.structure.OdbGraph;
import io.shiftleft.overflowdb.testdomains.simple.OdbTestEdge;
import io.shiftleft.overflowdb.testdomains.simple.OdbTestNode;
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
    try (OdbGraph graph = newGraph()) {
      NodeSerializer serializer = new NodeSerializer();
      NodeDeserializer deserializer = newDeserializer(graph);
      Vertex vertexRef = graph.addVertex(
          T.label, OdbTestNode.LABEL,
          OdbTestNode.STRING_PROPERTY, "StringValue",
          OdbTestNode.INT_PROPERTY, 42,
          OdbTestNode.STRING_LIST_PROPERTY, Arrays.asList("stringOne", "stringTwo"),
          OdbTestNode.INT_LIST_PROPERTY, Arrays.asList(42, 43)
      );

      OdbTestNode underlyingVertexDb = ((NodeRef<OdbTestNode>) vertexRef).get();
      byte[] bytes = serializer.serialize(underlyingVertexDb);
      Vertex deserialized = deserializer.deserialize(bytes);

      assertEquals(underlyingVertexDb.id(), deserialized.id());
      assertEquals(underlyingVertexDb.label(), deserialized.label());
      assertEquals(underlyingVertexDb.valueMap(), ((OdbTestNode) deserialized).valueMap());

      final NodeRef deserializedRef = deserializer.deserializeRef(bytes);
      assertEquals(vertexRef.id(), deserializedRef.id);
      assertEquals(OdbTestNode.LABEL, deserializedRef.label());
    }
  }

  @Test
  public void serializeWithEdge() throws IOException {
    try (OdbGraph graph = newGraph()) {
      NodeSerializer serializer = new NodeSerializer();
      NodeDeserializer deserializer = newDeserializer(graph);

      Vertex v0 = graph.addVertex(T.label, OdbTestNode.LABEL);
      Vertex v1 = graph.addVertex(T.label, OdbTestNode.LABEL);
      Edge edge = v0.addEdge(OdbTestEdge.LABEL, v1, OdbTestEdge.LONG_PROPERTY, Long.MAX_VALUE);

      OdbTestNode v0Underlying = ((NodeRef<OdbTestNode>) v0).get();
      OdbTestNode v1Underlying = ((NodeRef<OdbTestNode>) v1).get();
      Vertex v0Deserialized = deserializer.deserialize(serializer.serialize(v0Underlying));
      Vertex v1Deserialized = deserializer.deserialize(serializer.serialize(v1Underlying));

      Edge edgeViaV0Deserialized = v0Deserialized.edges(Direction.OUT, OdbTestEdge.LABEL).next();
      Edge edgeViaV1Deserialized = v1Deserialized.edges(Direction.IN, OdbTestEdge.LABEL).next();

      assertEquals(edge.id(), edgeViaV0Deserialized.id());
      assertEquals(edge.id(), edgeViaV1Deserialized.id());
      assertEquals(OdbTestEdge.LABEL, edgeViaV0Deserialized.label());
      assertEquals(OdbTestEdge.LABEL, edgeViaV1Deserialized.label());
      assertEquals(Long.MAX_VALUE, (long) edgeViaV0Deserialized.value(OdbTestEdge.LONG_PROPERTY));
      assertEquals(Long.MAX_VALUE, (long) edgeViaV1Deserialized.value(OdbTestEdge.LONG_PROPERTY));

      assertEquals(v0.id(), edgeViaV0Deserialized.outVertex().id());
      assertEquals(v1.id(), edgeViaV0Deserialized.inVertex().id());
      assertEquals(v0.id(), edgeViaV1Deserialized.outVertex().id());
      assertEquals(v1.id(), edgeViaV1Deserialized.inVertex().id());
    }
  }

  private NodeDeserializer newDeserializer(OdbGraph graph) {
    Map<String, NodeFactory> vertexFactories = new HashMap();
    vertexFactories.put(OdbTestNode.LABEL, OdbTestNode.factory);
    return new NodeDeserializer(graph, vertexFactories);
  }

  private OdbGraph newGraph() {
    return OdbGraph.open(
        OdbConfig.withoutOverflow(),
        Arrays.asList(OdbTestNode.factory),
        Arrays.asList(OdbTestEdge.factory)
    );
  }

}
