package overflowdb;

public class Property<A> {
  public final PropertyKey key;
  public final A value;

  public Property(PropertyKey key, A value) {
    this.key = key;
    this.value = value;
  }

  public Property(String key, A value) {
    this.key = new PropertyKey(key);
    this.value = value;
  }
}
