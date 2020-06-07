package overflowdb.util;

/**
 * Int array that stores values in byte[] until number
 * larger than min/max byte is written, then it grows to short[]
 * and if number larger than short is written then it grows to int[].
 * For the specific case with the OdbNode.edgeOffsets where the most
 * numbers fit into byte almost all the time it saves ~90Mb in a 1.3Gb heap.
 */
public class PackedIntArray {
  private static final byte BYTE_ARRAY = 0;
  private static final byte SHORT_ARRAY = 1;
  private static final byte INT_ARRAY = 2;
  private static final PackedIntArray EMPTY = new PackedIntArray(0);

  private byte kind;
  private Object underlying;

  public static PackedIntArray create(int length) {
    if (length == 0)
      return EMPTY;
    else
      return new PackedIntArray(length);
  }

  public static PackedIntArray of(int... values) {
    if (values.length == 0)
      return EMPTY;
    else
      return new PackedIntArray(values);
  }

  interface ArrayAccessor {
    Object allocate(int length);
    int get(Object arr, int index);
    void set(Object arr, int index, int value);
    int length(Object arr);
  }

  public PackedIntArray(int length) {
    this.kind = BYTE_ARRAY;
    this.underlying = getArrayAccessor(this.kind).allocate(length);

  }

  private ArrayAccessor getArrayAccessor(byte kind) {
    switch (kind) {
      case BYTE_ARRAY: return ByteArrayAccessor.INSTANCE;
      case SHORT_ARRAY: return ShortArrayAccessor.INSTANCE;
      case INT_ARRAY: return IntArrayAccessor.INSTANCE;
      default: throw new java.lang.IllegalStateException("PackedIntArray.kind has incorrect value.");
    }
  }

  public PackedIntArray(int[] source) {
    this.kind = BYTE_ARRAY;
    this.underlying = getArrayAccessor(this.kind).allocate(source.length);
    for (int i = 0; i < length(); ++i)
      set(i, source[i]);
  }

  public int length() {
    return getArrayAccessor(this.kind).length(this.underlying);
  }

  public int get(int index) {
    return getArrayAccessor(this.kind).get(this.underlying, index);
  }

  public void set(int index, int value) {
    final byte requiredKind = arrayKindForValue(value);
    if (requiredKind > this.kind) {
      this.underlying = grow(requiredKind);
      this.kind = requiredKind;
    }
    getArrayAccessor(this.kind).set(this.underlying, index, value);
  }

  private Object grow(byte requiredKind) {
    assert kind < requiredKind;
    Object newArray = getArrayAccessor(requiredKind).allocate(length());
    for (int i = 0; i < length(); ++i)
      getArrayAccessor(requiredKind).set(newArray, i, get(i));
    return newArray;
  }

  private byte arrayKindForValue(int value) {
    if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE)
      return BYTE_ARRAY;
    else if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE)
      return SHORT_ARRAY;
    else
      return INT_ARRAY;
  }

  public int[] toIntArray() {
    if (this.kind == INT_ARRAY)
      return (int[])this.underlying;
    else {
      return (int[])this.grow(INT_ARRAY);
    }
  }

  private static class IntArrayAccessor implements ArrayAccessor {
    static final IntArrayAccessor INSTANCE = new IntArrayAccessor();

    private int[] cast(Object o) { return (int[])o; }

    @Override
    public Object allocate(int length) {
      return new int[length];
    }

    @Override
    public int get(Object o, int index) {
      return cast(o)[index];
    }

    @Override
    public void set(Object o, int index, int value) {
      cast(o)[index] = value;
    }

    @Override
    public int length(Object o) {
      return cast(o).length;
    }
  }

  private static class ShortArrayAccessor implements ArrayAccessor {
    static final ShortArrayAccessor INSTANCE = new ShortArrayAccessor();

    private short[] cast(Object o) { return (short[])o; }

    @Override
    public Object allocate(int length) {
      return new short[length];
    }

    @Override
    public int get(Object o, int index) {
      return cast(o)[index];
    }

    @Override
    public void set(Object o, int index, int value) {
      cast(o)[index] = (short)value;
    }

    @Override
    public int length(Object o) {
      return cast(o).length;
    }
  }

  private static class ByteArrayAccessor implements ArrayAccessor {
    static final ByteArrayAccessor INSTANCE = new ByteArrayAccessor();

    private byte[] cast(Object o) { return (byte[])o; }

    @Override
    public Object allocate(int length) {
      return new byte[length];
    }

    @Override
    public int get(Object o, int index) {
      return cast(o)[index];
    }

    @Override
    public void set(Object o, int index, int value) {
      cast(o)[index] = (byte)value;
    }

    @Override
    public int length(Object o) {
      return cast(o).length;
    }
  }
}
