package overflowdb.storage;

import overflowdb.Node;
import overflowdb.NodeLayoutInformation;
import overflowdb.NodeDb;
import overflowdb.NodeRef;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class NodeSerializer extends BookKeeper {
  private final OdbStorage storage;

  public NodeSerializer(boolean statsEnabled, OdbStorage storage) {
    super(statsEnabled);
    this.storage = storage;
  }

  public byte[] serialize(NodeDb node) throws IOException {
    long startTimeNanos = getStartTimeNanos();
    try (MessageBufferPacker packer = MessagePack.newDefaultBufferPacker()) {
      NodeLayoutInformation layoutInformation = node.layoutInformation();
      /* marking as clean *before* we start serializing - if node is modified any time afterwards it'll be marked as dirty */
      node.markAsClean();

      packer.packLong(node.ref.id());

      final int labelId = storage.lookupOrCreateStringToIntMapping(layoutInformation.label);
      packer.packInt(labelId);

      packProperties(packer, node.propertiesMapWithoutDefaults(), node::convertPropertyForStorage);
      packEdges(packer, node);

      if (statsEnabled) recordStatistics(startTimeNanos);
      return packer.toByteArray();
    }
  }

  /**
   * when deserializing, msgpack can't differentiate between e.g. int and long, so we need to encode the type as well - doing that with an array
   * i.e. format is: Map[PropertyName, Array(TypeId, PropertyValue)]
   */
  private void packProperties(MessageBufferPacker packer, Map<String, Object> properties,
                              Function<Object, Object> convertPropertyForStorage) throws IOException {
    packer.packMapHeader(properties.size());
    for (Map.Entry<String, Object> property : properties.entrySet()) {
      int propertyKeyId = storage.lookupOrCreateStringToIntMapping(property.getKey());
      packer.packInt(propertyKeyId);
      Object valueForStorage = convertPropertyForStorage.apply(property.getValue());
      packTypedValue(packer, valueForStorage);
    }
  }

  private void packEdges(MessageBufferPacker packer, NodeDb node) throws IOException {
    NodeLayoutInformation layoutInformation = node.layoutInformation();
    int[] edgeOffsets = node.getEdgeOffsets();

    packEdgesForOneDirection(packer, node, layoutInformation.allowedOutEdgeLabels(), edgeOffsets, layoutInformation::outEdgeToOffsetPosition);
    packEdgesForOneDirection(packer, node, layoutInformation.allowedInEdgeLabels(), edgeOffsets, layoutInformation::inEdgeToOffsetPosition);
  }

  private void packEdgesForOneDirection(MessageBufferPacker packer, NodeDb node, String[] allowedEdgeLabels, int[] edgeOffsets, Function<String, Integer> edgeToOffsetPosition) throws IOException {
    // first prepare everything we want to write, so that we can prepend it with the length - helps during deserialization
    ArrayList<Object> edgeLabelAndOffsetPos = new ArrayList<>(allowedEdgeLabels.length * 2);
    int edgeTypeCount = 0;
    for (String edgeLabel : allowedEdgeLabels) {
      int offsetPos = edgeToOffsetPosition.apply(edgeLabel);
      int count = edgeOffsets[offsetPos * 2 + 1];
      if (count > 0) {
        edgeTypeCount++;
        edgeLabelAndOffsetPos.add(edgeLabel);
        edgeLabelAndOffsetPos.add(offsetPos);
      }
    }
    packer.packInt(edgeTypeCount);
    for (int i = 0; i < edgeLabelAndOffsetPos.size(); i += 2) {
      String edgeLabel = (String) edgeLabelAndOffsetPos.get(i);
      int offsetPos = (int) edgeLabelAndOffsetPos.get(i + 1);
      packEdgesForOneLabel(packer, node, edgeLabel, offsetPos);
    }
  }

  private void packEdgesForOneLabel(MessageBufferPacker packer, NodeDb node, String edgeLabel, int offsetPos) throws IOException {
    NodeLayoutInformation layoutInformation = node.layoutInformation();
    Object[] adjacentNodesWithEdgeProperties = node.getAdjacentNodesWithEdgeProperties();
    final Set<String> edgePropertyNames = layoutInformation.edgePropertyKeys(edgeLabel);

    // pointers into adjacentNodesWithEdgeProperties
    int start = node.startIndex(offsetPos);
    int blockLength = node.blockLength(offsetPos);
    int strideSize = node.getStrideSize(edgeLabel);

    // first prepare all edges to get total count, then first write the count and then the edges
    ArrayList<Object> adjacentNodeIdsAndProperties = new ArrayList<>(blockLength / strideSize);
    int edgeCount = 0;
    int currIdx = start;
    int endIdx = start + blockLength;
    while (currIdx < endIdx) {
      Node adjacentNode = (Node) adjacentNodesWithEdgeProperties[currIdx];
      if (adjacentNode != null) {
        edgeCount++;
        adjacentNodeIdsAndProperties.add(adjacentNode.id());

        Map<String, Object> edgeProperties = new HashMap<>();
        for (String propertyName : edgePropertyNames) {
          int edgePropertyOffset = layoutInformation.getEdgePropertyOffsetRelativeToAdjacentNodeRef(edgeLabel, propertyName);
          Object property = adjacentNodesWithEdgeProperties[currIdx + edgePropertyOffset];
          if (property != null) {
            edgeProperties.put(propertyName, property);
          }
        }
        adjacentNodeIdsAndProperties.add(edgeProperties);
      }
      currIdx += strideSize;
    }

    int labelId = storage.lookupOrCreateStringToIntMapping(edgeLabel);
    packer.packInt(labelId);
    packer.packInt(edgeCount);

    for (int edgeIdx = 0; edgeIdx < edgeCount; edgeIdx++) {
      long adjacentNodeId = (long) adjacentNodeIdsAndProperties.get(edgeIdx * 2);
      packer.packLong(adjacentNodeId);
      Map<String, Object> edgeProperties = (Map<String, Object>) adjacentNodeIdsAndProperties.get(edgeIdx * 2 + 1);
      packProperties(packer, edgeProperties, node::convertPropertyForStorage);
    }
  }

  /**
   * format: `[ValueType.id, value]`
   */
  private void packTypedValue(final MessageBufferPacker packer, final Object value) throws IOException {
    packer.packArrayHeader(2);
    if (value == null) {
      packer.packByte(ValueTypes.UNKNOWN.id);
      packer.packNil();
    } else if (value instanceof NodeRef) {
      packer.packByte(ValueTypes.NODE_REF.id);
      packer.packLong(((NodeRef) value).id());
    } else if (value instanceof Boolean) {
      packer.packByte(ValueTypes.BOOLEAN.id);
      packer.packBoolean((Boolean) value);
    } else if (value instanceof String) {
      packer.packByte(ValueTypes.STRING.id);
      packer.packString((String) value);
    } else if (value instanceof Byte) {
      packer.packByte(ValueTypes.BYTE.id);
      packer.packByte((byte) value);
    } else if (value instanceof Short) {
      packer.packByte(ValueTypes.SHORT.id);
      packer.packShort((short) value);
    } else if (value instanceof Integer) {
      packer.packByte(ValueTypes.INTEGER.id);
      packer.packInt((int) value);
    } else if (value instanceof Long) {
      packer.packByte(ValueTypes.LONG.id);
      packer.packLong((long) value);
    } else if (value instanceof Float) {
      packer.packByte(ValueTypes.FLOAT.id);
      packer.packFloat((float) value);
    } else if (value instanceof Double) {
      packer.packByte(ValueTypes.DOUBLE.id);
      packer.packDouble((double) value);
    } else if (value instanceof List) {
      packer.packByte(ValueTypes.LIST.id);
      List listValue = (List) value;
      packer.packArrayHeader(listValue.size());
      final Iterator listIter = listValue.iterator();
      while (listIter.hasNext()) {
        packTypedValue(packer, listIter.next());
      }
    } else if (value instanceof Character) {
      packer.packByte(ValueTypes.CHARACTER.id);
      packer.packInt((Character) value);
    } else {
      throw new UnsupportedOperationException("id type `" + value.getClass());
    }
  }

}
