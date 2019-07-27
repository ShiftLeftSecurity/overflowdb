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
package io.shiftleft.overflowdb.storage;

/* when serializing values we need to encode the id type in a separate entry, to ensure we can deserialize it
 * back to the very same type. I would have hoped that MsgPack does that for us, but that's only partly the case.
 * E.g. the different integer types cannot be distinguished other than by their value. When we deserialize `42`,
 * we have no idea whether it should be deserialized as a byte, short, integer or double */
public enum ValueTypes {
  BOOLEAN((byte) 0),
  STRING((byte) 1),
  BYTE((byte) 2),
  SHORT((byte) 3),
  INTEGER((byte) 4),
  LONG((byte) 5),
  FLOAT((byte) 6),
  DOUBLE((byte) 7),
  LIST((byte) 8),
  VERTEX_REF((byte) 9),
  UNKNOWN((byte) 10);

  public final byte id;
  ValueTypes(byte id) {
    this.id = id;
  }

  public static ValueTypes lookup(byte id) {
    switch (id) {
      case 0: return BOOLEAN;
      case 1: return STRING;
      case 2: return BYTE;
      case 3: return SHORT;
      case 4: return INTEGER;
      case 5: return LONG;
      case 6: return FLOAT;
      case 7: return DOUBLE;
      case 8: return LIST;
      case 9: return VERTEX_REF;
      case 10: return UNKNOWN;
      default: throw new IllegalArgumentException("unknown id type " + id);
    }
  }
}
