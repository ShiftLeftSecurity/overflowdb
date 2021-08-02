package overflowdb.testdomains.simple;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/** a funky list that supports only funky entries.. to test `NodeSerializer.convertPropertyForPersistence` */
public class FunkyList {
  private List<String> entries = new ArrayList();

  private static Set<String> funkyWords = new HashSet<String>() {{
    add("anthropomorphic");
    add("apoplectic");
    add("appaloosa");
    add("bedlam");
    add("boondoggle");
    add("bucolic");
  }};

  public FunkyList add(String value) {
    if (funkyWords.contains(value)) {
      entries.add(value);
      return this;
    } else throw new RuntimeException("not funky enough!");
  }

  public List<String> getEntries() {
    return new ArrayList<>(entries);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FunkyList funkyList = (FunkyList) o;
    return entries.equals(funkyList.entries);
  }

  @Override
  public int hashCode() {
    return Objects.hash(entries);
  }

  public static Function<FunkyList, String[]> toStorageType =
      funkyList -> funkyList.entries.toArray(new String[funkyList.entries.size()]);

}
