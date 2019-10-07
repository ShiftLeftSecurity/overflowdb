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
  NODE_REF((byte) 9),
  UNKNOWN((byte) 10),
  CHARACTER((byte) 11);

  public final byte id;

  ValueTypes(byte id) {
    this.id = id;
  }

  public static ValueTypes lookup(byte id) {
    switch (id) {
      case 0:
        return BOOLEAN;
      case 1:
        return STRING;
      case 2:
        return BYTE;
      case 3:
        return SHORT;
      case 4:
        return INTEGER;
      case 5:
        return LONG;
      case 6:
        return FLOAT;
      case 7:
        return DOUBLE;
      case 8:
        return LIST;
      case 9:
        return NODE_REF;
      case 10:
        return UNKNOWN;
      case 11:
        return CHARACTER;
      default:
        throw new IllegalArgumentException("unknown id type " + id);
    }
  }
}
