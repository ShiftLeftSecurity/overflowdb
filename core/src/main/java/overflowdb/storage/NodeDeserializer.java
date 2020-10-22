package overflowdb.storage;

import gnu.trove.map.hash.THashMap;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;
import overflowdb.Direction;
import overflowdb.NodeFactory;
import overflowdb.NodeRef;
import overflowdb.Graph;
import overflowdb.NodeDb;
import overflowdb.util.PropertyHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class NodeDeserializer extends BookKeeper {
  protected final Graph graph;
  private final Map<Integer, NodeFactory> nodeFactoryByLabelId;
  private ConcurrentHashMap<String, String> interner;

  public NodeDeserializer(Graph graph, Map<Integer, NodeFactory> nodeFactoryByLabelId, boolean statsEnabled) {
    super(statsEnabled);
    this.graph = graph;
    this.nodeFactoryByLabelId = nodeFactoryByLabelId;
    this.interner = new ConcurrentHashMap<>();
  }

  private final String intern(String s){
    String interned = interner.putIfAbsent(s, s);
    return interned == null ? s : interned;
  }

  public final NodeDb deserialize(byte[] bytes) throws IOException {
    long startTimeNanos = getStartTimeNanos();
    if (null == bytes)
      return null;

    MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(bytes);
    final long id = unpacker.unpackLong();
    final int labelId = unpacker.unpackInt();
    final Map<String, Object> properties = unpackPropertiesAsMap(unpacker);
//  // TODO drop
//    final int[] edgeOffsets = unpackEdgeOffsets(unpacker);
//    final Object[] adjacentNodesWithProperties = unpackAdjacentNodesWithProperties(unpacker);
//    NodeDb node = createNode(id, labelId, properties, edgeOffsets, adjacentNodesWithProperties);

    // TODO refactor/extract / move to NodeDb to keep internals in one place?
    NodeDb node = getNodeFactory(labelId).createNode(graph, id);
    PropertyHelper.attachProperties(node, toKeyValueArray(properties));

    int outEdgeTypesCount = unpacker.unpackArrayHeader();
    for (int outEdgeTypeIdx = 0; outEdgeTypeIdx < outEdgeTypesCount; outEdgeTypeIdx++) {
      String edgeLabel = unpacker.unpackString();
      int edgeCount = unpacker.unpackArrayHeader();
      for (int edgeIdx = 0; edgeIdx < edgeCount; edgeIdx++) {
        long adjancentNodeId = unpacker.unpackLong();
        NodeRef adjacentNode = (NodeRef) graph.node(adjancentNodeId);
        Object[] edgeProperties = unpackProperties(unpacker);
        node.storeAdjacentNode(Direction.OUT, edgeLabel, adjacentNode, edgeProperties);
      }
    }

    // same for IN edges // TODO remove duplication
    int inEdgeTypesCount = unpacker.unpackArrayHeader();
    for (int inEdgeTypeIdx = 0; inEdgeTypeIdx < inEdgeTypesCount; inEdgeTypeIdx++) {
      String edgeLabel = unpacker.unpackString();
      int edgeCount = unpacker.unpackArrayHeader();
      for (int edgeIdx = 0; edgeIdx < edgeCount; edgeIdx++) {
        long adjancentNodeId = unpacker.unpackLong();
        NodeRef adjacentNode = (NodeRef) graph.node(adjancentNodeId);
        final Object[] edgeProperties = unpackProperties(unpacker);
        node.storeAdjacentNode(Direction.IN, edgeLabel, adjacentNode, edgeProperties);
      }
    }

    node.markAsClean();

    if (statsEnabled) recordStatistics(startTimeNanos);
    return node;
  }

  /**
   * only deserialize the part we're keeping in memory, used during startup when initializing from disk
   */
  public final NodeRef deserializeRef(byte[] bytes) throws IOException {
    try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(bytes)) {
      long id = unpacker.unpackLong();
      int labelId = unpacker.unpackInt();

      return createNodeRef(id, labelId);
    }
  }

  // TODO remove this - use `unpackProperties` everywhere
  private final Map<String, Object> unpackPropertiesAsMap(MessageUnpacker unpacker) throws IOException {
    int propertyCount = unpacker.unpackMapHeader();
    Map<String, Object> res = new THashMap<>(propertyCount);
    for (int i = 0; i < propertyCount; i++) {
      final String key = intern(unpacker.unpackString());
      final Object unpackedProperty = unpackValue(unpacker.unpackValue().asArrayValue());
      res.put(key, unpackedProperty);
    }
    return res;
  }

  private final Object[] unpackProperties(MessageUnpacker unpacker) throws IOException {
    int propertyCount = unpacker.unpackMapHeader();
    Object[] res = new Object[propertyCount];
    int resIdx = 0;
    for (int propertyIdx = 0; propertyIdx < propertyCount; propertyIdx++) {
      final String key = intern(unpacker.unpackString());
      final Object unpackedProperty = unpackValue(unpacker.unpackValue().asArrayValue());
      res[resIdx++] = key;
      res[resIdx++] = unpackedProperty;
    }
    return res;
  }

//  // TODO drop
//  private final int[] unpackEdgeOffsets(MessageUnpacker unpacker) throws IOException {
//    int size = unpacker.unpackArrayHeader();
//    int[] edgeOffsets = new int[size];
//    for (int i = 0; i < size; i++) {
//      edgeOffsets[i] = unpacker.unpackInt();
//    }
//    return edgeOffsets;
//  }
//
//  // TODO drop
//  protected final Object[] unpackAdjacentNodesWithProperties(MessageUnpacker unpacker) throws IOException {
//    int size = unpacker.unpackArrayHeader();
//    Object[] adjacentNodesWithProperties = new Object[size];
//    for (int i = 0; i < size; i++) {
//      adjacentNodesWithProperties[i] = unpackValue(unpacker.unpackValue().asArrayValue());
//    }
//    return adjacentNodesWithProperties;
//  }

  private final Object unpackValue(final ArrayValue packedValueAndType) {
    final Iterator<Value> iter = packedValueAndType.iterator();
    final byte valueTypeId = iter.next().asIntegerValue().asByte();
    final Value value = iter.next();

    switch (ValueTypes.lookup(valueTypeId)) {
      case UNKNOWN:
        return null;
      case NODE_REF:
        long id = value.asIntegerValue().asLong();
        return graph.node(id);
      case BOOLEAN:
        return value.asBooleanValue().getBoolean();
      case STRING:
        return intern(value.asStringValue().asString());
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
      case CHARACTER:
        return (char) value.asIntegerValue().asInt();
      default:
        throw new UnsupportedOperationException("unknown valueTypeId=`" + valueTypeId);
    }
  }

  protected final Object[] toKeyValueArray(Map<String, Object> properties) {
    List keyValues = new ArrayList(properties.size() * 2); // may grow bigger if there's list entries
    for (Map.Entry<String, Object> entry : properties.entrySet()) {
      //todo: We fail to properly intern strings contained in a List.
      final String key = intern(entry.getKey());
      final Object property = entry.getValue();
      keyValues.add(key);
      if(property instanceof String)
        keyValues.add(intern((String)property));
      else
        keyValues.add(property);
    }
    return keyValues.toArray();
  }

  protected final NodeRef createNodeRef(long id, int labelId) {
    return getNodeFactory(labelId).createNodeRef(graph, id);
  }

//  // TODO drop
//  protected final NodeDb createNode(long id, int labelId, Map<String, Object> properties, int[] edgeOffsets, Object[] adjacentNodesWithProperties) {
//    NodeDb node = getNodeFactory(labelId).createNode(graph, id);
//    PropertyHelper.attachProperties(node, toKeyValueArray(properties));
//    node.setEdgeOffsets(edgeOffsets);
//    node.setAdjacentNodesWithEdgeProperties(adjacentNodesWithProperties);
//    node.markAsClean();
//
//    return node;
//  }

  private final NodeFactory getNodeFactory(int labelId) {
    if (!nodeFactoryByLabelId.containsKey(labelId))
      throw new AssertionError("nodeFactory not found for labelId=" + labelId);

    return nodeFactoryByLabelId.get(labelId);
  }

}
