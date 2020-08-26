package overflowdb.util;

import overflowdb.Node;

import java.util.Iterator;
import java.util.Map;

public class PropertyHelper {

  public static void attachProperties(Node node, Object... keyValues) {
    if (keyValues.length % 2 != 0)
      throw new IllegalArgumentException("The provided key/value array length must be a multiple of two");

    for (int i = 0; i < keyValues.length; i = i + 2) {
      Object key = keyValues[i];
      Object value = keyValues[i + 1];
      if (!(key instanceof String))
        throw new IllegalArgumentException(String.format("The provided key must be of type `String`, but was: $s (value=$s)", key.getClass(), key.toString()));
      node.setProperty((String) key, value);
    }
  }

  public static final Object[] toKeyValueArray(Map<String, Object> keyValues) {
    final Object[] keyValuesArray = new Object[keyValues.size() * 2];
    int i = 0;
    final Iterator<Map.Entry<String, Object>> iterator = keyValues.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, Object> entry = iterator.next();
      keyValuesArray[i++] = entry.getKey();
      keyValuesArray[i++] = entry.getValue();
    }
    return keyValuesArray;
  }
}
