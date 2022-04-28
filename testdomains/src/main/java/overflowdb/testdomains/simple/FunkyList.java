package overflowdb.testdomains.simple;

import java.util.*;
import java.util.function.Function;

/** a funky list that supports only funky entries.. to test `NodeSerializer.convertPropertyForPersistence` */
public class FunkyList extends AbstractCollection<String> {

  private List<String> entries = new ArrayList();

  private static Set<String> funkyWords = new HashSet<String>() {{
    add("anthropomorphic");
    add("apoplectic");
    add("appaloosa");
    add("bedlam");
    add("boondoggle");
    add("bucolic");
  }};

  public boolean add(String value) {
    if (funkyWords.contains(value)) {
      entries.add(value);
      return true;
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

  @Override
  public Iterator<String> iterator() {
    return entries.iterator();
  }

  @Override
  public int size() {
    return entries.size();
  }

}
