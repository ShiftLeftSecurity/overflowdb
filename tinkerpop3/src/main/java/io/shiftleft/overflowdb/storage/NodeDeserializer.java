package io.shiftleft.overflowdb.storage;

import gnu.trove.map.hash.THashMap;
import io.shiftleft.overflowdb.structure.ElementRef;
import io.shiftleft.overflowdb.structure.OverflowDb;
import io.shiftleft.overflowdb.structure.OverflowDbNode;
import io.shiftleft.overflowdb.structure.OverflowElementFactory;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class NodeDeserializer {
  private final Logger logger = LoggerFactory.getLogger(getClass());
  protected final OverflowDb graph;
  protected final Map<String, OverflowElementFactory.ForNode> vertexFactoryByLabel;
  private int deserializedCount = 0;
  private long deserializationTimeSpentMillis = 0;

  public NodeDeserializer(OverflowDb graph, Map<String, OverflowElementFactory.ForNode> vertexFactoryByLabel) {
    this.graph = graph;
    this.vertexFactoryByLabel = vertexFactoryByLabel;
  }

  public OverflowDbNode deserialize(byte[] bytes) throws IOException {
    long start = System.currentTimeMillis();
    if (null == bytes)
      return null;

    try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(bytes)) {
      final long id = unpacker.unpackLong();
      final String label = unpacker.unpackString();
      final Map<String, Object> properties = unpackProperties(unpacker);
      final int[] edgeOffsets = unpackEdgeOffsets(unpacker);
      final Object[] adjacentVerticesWithProperties = unpackAdjacentVerticesWithProperties(unpacker);

      OverflowDbNode node = createNode(id, label, properties, edgeOffsets, adjacentVerticesWithProperties);

      deserializedCount++;
      deserializationTimeSpentMillis += System.currentTimeMillis() - start;
      if (deserializedCount % 131072 == 0) { //2^17
        float avgDeserializationTime = deserializationTimeSpentMillis / (float) deserializedCount;
        logger.debug("stats: deserialized " + deserializedCount + " vertices in total (avg time: " + avgDeserializationTime + "ms)");
      }
      return node;
    }
  }

  /**
   * only deserialize the part we're keeping in memory, used during startup when initializing from disk
   */
  public ElementRef deserializeRef(byte[] bytes) throws IOException {
    try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(bytes)) {
      long id = unpacker.unpackLong();
      String label = unpacker.unpackString();

      return createNodeRef(id, label);
    }
  }

  private Map<String, Object> unpackProperties(MessageUnpacker unpacker) throws IOException {
    int propertyCount = unpacker.unpackMapHeader();
    Map<String, Object> res = new THashMap<>(propertyCount);
    for (int i = 0; i < propertyCount; i++) {
      final String key = unpacker.unpackString();
      final Object unpackedProperty = unpackValue(unpacker.unpackValue().asArrayValue());
      res.put(key, unpackedProperty);
    }
    return res;
  }

  private int[] unpackEdgeOffsets(MessageUnpacker unpacker) throws IOException {
    int size = unpacker.unpackArrayHeader();
    int[] edgeOffsets = new int[size];
    for (int i = 0; i < size; i++) {
      edgeOffsets[i] = unpacker.unpackInt();
    }
    return edgeOffsets;
  }

  protected Object[] unpackAdjacentVerticesWithProperties(MessageUnpacker unpacker) throws IOException {
    int size = unpacker.unpackArrayHeader();
    Object[] adjacentVerticesWithProperties = new Object[size];
    for (int i = 0; i < size; i++) {
      adjacentVerticesWithProperties[i] = unpackValue(unpacker.unpackValue().asArrayValue());
    }
    return adjacentVerticesWithProperties;
  }

  private Object unpackValue(final ArrayValue packedValueAndType) {
    final Iterator<Value> iter = packedValueAndType.iterator();
    final byte valueTypeId = iter.next().asIntegerValue().asByte();
    final Value value = iter.next();

    switch (ValueTypes.lookup(valueTypeId)) {
      case UNKNOWN:
        return null;
      case VERTEX_REF:
        long id = value.asIntegerValue().asLong();
        return graph.vertex(id);
      case BOOLEAN:
        return value.asBooleanValue().getBoolean();
      case STRING:
        return value.asStringValue().asString();
      case BYTE:
        return value.asIntegerValue().asByte();
      case SHORT:
        return value.asIntegerValue().asShort();
      case INTEGER:
        return value.asIntegerValue().asInt();
      case LONG:
        return value.asIntegerValue().asLong();
      case FLOAT:
        return value.asFloatValue().toFloat();
      case DOUBLE:
        return Double.valueOf(value.asFloatValue().toFloat());
      case LIST:
        final ArrayValue arrayValue = value.asArrayValue();
        List deserializedArray = new ArrayList(arrayValue.size());
        final Iterator<Value> valueIterator = arrayValue.iterator();
        while (valueIterator.hasNext()) {
          deserializedArray.add(unpackValue(valueIterator.next().asArrayValue()));
        }
        return deserializedArray;
      default:
        throw new NotImplementedException("unknown valueTypeId=`" + valueTypeId);
    }
  }

  protected Object[] toTinkerpopKeyValues(Map<String, Object> properties) {
    List keyValues = new ArrayList(properties.size() * 2); // may grow bigger if there's list entries
    for (Map.Entry<String, Object> entry : properties.entrySet()) {
      final String key = entry.getKey();
      final Object property = entry.getValue();
      // special handling for lists: create separate key/value entry for each list entry
      if (property instanceof List) {
        for (Object value : (List) property) {
          keyValues.add(key);
          keyValues.add(value);
        }
      } else {
        keyValues.add(key);
        keyValues.add(property);
      }
    }
    return keyValues.toArray();
  }

  protected ElementRef createNodeRef(long id, String label) {
    OverflowElementFactory.ForNode vertexFactory = vertexFactoryByLabel.get(label);
    if (vertexFactory == null) {
      throw new AssertionError("vertexFactory not found for label=" + label);
    }

    return vertexFactory.createVertexRef(id, graph);
  }

  protected OverflowDbNode createNode(long id, String label, Map<String, Object> properties, int[] edgeOffsets, Object[] adjacentVerticesWithProperties) {
    OverflowElementFactory.ForNode vertexFactory = vertexFactoryByLabel.get(label);
    if (vertexFactory == null) {
      throw new AssertionError("vertexFactory not found for label=" + label);
    }
    OverflowDbNode vertex = vertexFactory.createVertex(id, graph);
    ElementHelper.attachProperties(vertex, VertexProperty.Cardinality.list, toTinkerpopKeyValues(properties));
    vertex.setEdgeOffsets(edgeOffsets);
    vertex.setAdjacentVerticesWithProperties(adjacentVerticesWithProperties);

    return vertex;
  }

}
