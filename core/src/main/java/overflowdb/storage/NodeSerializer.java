package overflowdb.storage;

import overflowdb.NodeRef;
import overflowdb.NodeDb;
import overflowdb.util.PackedIntArray;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class NodeSerializer extends BookKeeper {
  public NodeSerializer(boolean statsEnabled) {
    super(statsEnabled);
  }

  public byte[] serialize(NodeDb node) throws IOException {
    long startTimeNanos = getStartTimeNanos();
    try (MessageBufferPacker packer = MessagePack.newDefaultBufferPacker()) {
      /* marking as clean *before* we start serializing - if node is modified any time afterwards it'll be marked as dirty */
      node.markAsClean();

      packer.packLong(node.ref.id);
      packer.packInt(node.layoutInformation().labelId);

      packProperties(packer, node.valueMap());
      packEdgeOffsets(packer, node.getEdgeOffsetsPackedArray());
      packAdjacentNodesWithProperties(packer, node.getAdjacentNodesWithProperties());

      if (statsEnabled) recordStatistics(startTimeNanos);
      return packer.toByteArray();
    }
  }

  /**
   * when deserializing, msgpack can't differentiate between e.g. int and long, so we need to encode the type as well - doing that with an array
   * i.e. format is: Map[PropertyName, Array(TypeId, PropertyValue)]
   */
  private void packProperties(MessageBufferPacker packer, Map<String, Object> properties) throws IOException {
    packer.packMapHeader(properties.size());
    for (Map.Entry<String, Object> property : properties.entrySet()) {
      packer.packString(property.getKey());
      packTypedValue(packer, property.getValue());
    }
  }

  private void packEdgeOffsets(MessageBufferPacker packer, PackedIntArray edgeOffsets) throws IOException {
    packer.packArrayHeader(edgeOffsets.length());
    for (int i = 0; i < edgeOffsets.length(); i++) {
      packer.packInt(edgeOffsets.get(i));
    }
  }

  private void packAdjacentNodesWithProperties(MessageBufferPacker packer, Object[] adjacentNodesWithProperties) throws IOException {
    packer.packArrayHeader(adjacentNodesWithProperties.length);
    for (int i = 0; i < adjacentNodesWithProperties.length; i++) {
      packTypedValue(packer, adjacentNodesWithProperties[i]);
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
      packer.packLong(((NodeRef) value).id);
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
      packer.packFloat((float) value); //msgpack doesn't support double, but we still want to deserialize it as a double later
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
      throw new UnsupportedOperationException("id type `" + value.getClass() + "` not yet supported");
    }
  }

}
