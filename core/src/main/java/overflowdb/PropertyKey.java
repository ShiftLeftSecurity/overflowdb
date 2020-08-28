package overflowdb;

public class PropertyKey<A> {
  public final String name;

  public PropertyKey(String name) {
    this.name = name;
  }

  public Property<A> of(A value) {
    return new Property<>(this, value);
  }
}
