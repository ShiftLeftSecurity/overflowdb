package overflowdb;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public abstract class Element {
  public abstract String label();

  public abstract Graph graph();

  public abstract Set<String> propertyKeys();

  public abstract Object property(String key);

  public <A> A property(String key, A defaultValue) {
    Object value = property(key);
    return value != null ? (A) value : defaultValue;
  }

  public abstract <A> A property(PropertyKey<A> key);

  public <A> A property(PropertyKey<A> key, A defaultValue) {
    Object value = property(key);
    return value != null ? (A) value : defaultValue;
  }

  /** override this in specific element class, to define a default value */
  protected Object propertyDefaultValue(String propertyKey) {
    return null;
  }

  public abstract <A> Optional<A> propertyOption(PropertyKey<A> key);

  public abstract Optional<Object> propertyOption(String key);

  /** Map with all properties, including the default property values which haven't been explicitly set */
  public abstract Map<String, Object> propertyMap();

  public abstract void setProperty(String key, Object value);

  public abstract <A> void setProperty(PropertyKey<A> key, A value);

  public abstract void setProperty(Property<?> property);

  public abstract void removeProperty(String key);

  public abstract void remove();
}
