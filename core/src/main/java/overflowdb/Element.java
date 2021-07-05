package overflowdb;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public abstract class Element {
  public abstract String label();

  public abstract Graph graph();

  public abstract Set<String> propertyKeys();

  public abstract Object property(String key);

  public abstract <A> A property(PropertyKey<A> key);

  public abstract <A> Optional<A> propertyOption(PropertyKey<A> key);

  public abstract Optional<Object> propertyOption(String key);

  /** Map with all properties */
  public abstract Map<String, Object> propertyMap();

  public abstract void setProperty(String key, Object value);

  public abstract <A> void setProperty(PropertyKey<A> key, A value);

  public abstract void setProperty(Property<?> property);

  public abstract void removeProperty(String key);

  public abstract void remove();
}
