package overflowdb.util;

import java.util.concurrent.ConcurrentHashMap;

public class StringInterner {
  private static ConcurrentHashMap<String, String> internedStrings = new ConcurrentHashMap<>();

  public static String intern(String s){
    String interned = internedStrings.putIfAbsent(s, s);
    return interned == null ? s : interned;
  }

}
