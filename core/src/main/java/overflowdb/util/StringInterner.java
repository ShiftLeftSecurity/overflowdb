package overflowdb.util;

import java.util.concurrent.ConcurrentHashMap;

public class StringInterner {
  private ConcurrentHashMap<String, String> internedStrings = new ConcurrentHashMap<>();

  public String intern(String s){
    String interned = internedStrings.putIfAbsent(s, s);
    return interned == null ? s : interned;
  }

  public void clear() {
    internedStrings.clear();
  }
}
