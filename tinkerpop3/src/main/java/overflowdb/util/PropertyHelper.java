package overflowdb.util;

import java.util.Iterator;
import java.util.Map;

public class PropertyHelper {

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
