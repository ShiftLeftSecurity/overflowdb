package overflowdb.util;

import java.util.Arrays;
import java.util.Iterator;

public class IteratorUtils {
  public static <A> Iterator<A> from (A... as) {
    Arrays.stream(as).iterator();
  }
}
