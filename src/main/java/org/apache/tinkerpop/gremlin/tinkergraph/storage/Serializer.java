/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.tinkergraph.storage;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.set.TLongSet;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class Serializer<A> {
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private int serializedCount = 0;
  private long serializationTimeSpentMillis = 0;

  protected abstract long getId(A a);
  protected abstract String getLabel(A a);
  protected abstract Map<String, Object> getProperties(A a);
  protected abstract Map<String, TLongSet> getEdgeIds(A a, Direction direction);

  public byte[] serialize(A a) throws IOException {
    long start = System.currentTimeMillis();
    try (MessageBufferPacker packer = MessagePack.newDefaultBufferPacker()) {
      packer.packLong(getId(a));
      packer.packString(getLabel(a));

      packEdgeIds(packer, getEdgeIds(a, Direction.IN));
      packEdgeIds(packer, getEdgeIds(a, Direction.OUT));
      packProperties(packer, getProperties(a));

      serializedCount++;
      serializationTimeSpentMillis += System.currentTimeMillis() - start;
      if (serializedCount % 100000 == 0) {
        float avgSerializationTime = serializationTimeSpentMillis / (float) serializedCount;
        logger.debug("stats: serialized " + serializedCount + " instances in total (avg time: " + avgSerializationTime + "ms)");
      }
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
      packPropertyValue(packer, property.getValue());
    }
  }

  /**
   * format: `[ValueType.id, value]`
   */
  private void packPropertyValue(final MessageBufferPacker packer, final Object value) throws IOException {
    packer.packArrayHeader(2);
    if (value instanceof Boolean) {
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
        packPropertyValue(packer, listIter.next());
      }
    } else {
      throw new NotImplementedException("id type `" + value.getClass() + "` not yet supported");
    }
  }

  /**
   * format: two `Map<Label, Array<EdgeId>>`, i.e. one Map for `IN` and one for `OUT` edges
   */
  private void packEdgeIds(final MessageBufferPacker packer,
                           final Map<String, TLongSet> edgeIdsByLabel) throws IOException {
    packer.packMapHeader(edgeIdsByLabel.size());
    for (Map.Entry<String, TLongSet> entry : edgeIdsByLabel.entrySet()) {
      final String label = entry.getKey();
      packer.packString(label);
      final TLongSet edgeIds = entry.getValue();
      packer.packArrayHeader(edgeIds.size());
      final TLongIterator edgeIdIter = edgeIds.iterator();
      while (edgeIdIter.hasNext()) {
        packer.packLong(edgeIdIter.next());
      }
    }
  }

}
