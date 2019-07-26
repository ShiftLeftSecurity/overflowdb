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

import gnu.trove.map.hash.THashMap;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.ElementRef;
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

public abstract class Deserializer<A> {
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private int deserializedCount = 0;
  private long deserializationTimeSpentMillis = 0;

  protected abstract boolean elementRefRequiresAdjacentElements();

  /** edgeId maps are passed dependent on `elementRefRequiresAdjacentElements`*/
  protected abstract ElementRef createElementRef(long id,
                                                 String label,
                                                 Map<String, long[]> inEdgeIdsByLabel,
                                                 Map<String, long[]> outEdgeIdsByLabel);

  protected abstract A createElement(long id,
                                     String label,
                                     Map<String, Object> properties,
                                     Map<String, long[]> inEdgeIdsByLabel,
                                     Map<String, long[]> outEdgeIdsByLabel);

  public A deserialize(byte[] bytes) throws IOException {
    long start = System.currentTimeMillis();
    if (null == bytes)
      return null;

    try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(bytes)) {
      final long id = unpacker.unpackLong();
      final String label = unpacker.unpackString();
      final Map<String, long[]> inEdgeIdsByLabel = unpackEdgeIdsByLabel(unpacker);
      final Map<String, long[]> outEdgeIdsByLabel = unpackEdgeIdsByLabel(unpacker);
      final Map<String, Object> properties = unpackProperties(unpacker);

      A a = createElement(id, label, properties, inEdgeIdsByLabel, outEdgeIdsByLabel);

      deserializedCount++;
      deserializationTimeSpentMillis += System.currentTimeMillis() - start;
      if (deserializedCount % 100000 == 0) {
        float avgDeserializationTime = deserializationTimeSpentMillis / (float) deserializedCount;
        logger.debug("stats: deserialized " + deserializedCount + " vertices in total (avg time: " + avgDeserializationTime + "ms)");
      }
      return a;
    }
  }

  /**
   * only deserialize the part we're keeping in memory, used during startup when initializing from disk
   */
  public ElementRef deserializeRef(byte[] bytes) throws IOException {
    try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(bytes)) {
      long id = unpacker.unpackLong();
      String label = unpacker.unpackString();

      Map<String, long[]> inEdgeIdsByLabel = null;
      Map<String, long[]> outEdgeIdsByLabel = null;
      if (elementRefRequiresAdjacentElements()) {
        inEdgeIdsByLabel = unpackEdgeIdsByLabel(unpacker);
        outEdgeIdsByLabel = unpackEdgeIdsByLabel(unpacker);
      }
      return createElementRef(id, label, inEdgeIdsByLabel, outEdgeIdsByLabel);
    }
  }

  private Map<String, Object> unpackProperties(MessageUnpacker unpacker) throws IOException {
    int propertyCount = unpacker.unpackMapHeader();
    Map<String, Object> res = new THashMap<>(propertyCount);
    for (int i = 0; i < propertyCount; i++) {
      final String key = unpacker.unpackString();
      final Object unpackedProperty = unpackProperty(unpacker.unpackValue().asArrayValue());
      res.put(key, unpackedProperty);
    }
    return res;
  }

  private Object unpackProperty(final ArrayValue packedValueAndType) {
    final Iterator<Value> iter = packedValueAndType.iterator();
    final byte valueTypeId = iter.next().asIntegerValue().asByte();
    final Value value = iter.next();

    switch (ValueTypes.lookup(valueTypeId)) {
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
          deserializedArray.add(unpackProperty(valueIterator.next().asArrayValue()));
        }
        return deserializedArray;
      default:
        throw new NotImplementedException("unknown valueTypeId=`" + valueTypeId);
    }
  }

  /**
   * format: `Map<Label, Array<EdgeId>>`
   */
  private Map<String, long[]> unpackEdgeIdsByLabel(MessageUnpacker unpacker) throws IOException {
    int labelCount = unpacker.unpackMapHeader();
    Map<String, long[]> edgeIdsByLabel = new THashMap<>(labelCount);
    for (int i = 0; i < labelCount; i++) {
      String label = unpacker.unpackString();
      int edgeIdsCount = unpacker.unpackArrayHeader();
      long[] edgeIds = new long[edgeIdsCount];
      for (int j = 0; j < edgeIdsCount; j++) {
        edgeIds[j] = unpacker.unpackLong();
      }
      edgeIdsByLabel.put(label, edgeIds);
    }
    return edgeIdsByLabel;
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


}


