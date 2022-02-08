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

  public <A> A property(String key, A defaultValue) {
    Object value = property(key);
    return value != null ? (A) value : defaultValue;
  }

  public <A> A property(PropertyKey<A> key, A defaultValue) {
    Object value = property(key);
    return value != null ? (A) value : defaultValue;
  }

  /** override this in specific element class, to define a default value */
  public Object propertyDefaultValue(String propertyKey) {
    return null;
  }

  public abstract <A> Optional<A> propertyOption(PropertyKey<A> key);

  public abstract Optional<Object> propertyOption(String key);

  /** Map with all properties, including the default property values which haven't been explicitly set */
  public abstract Map<String, Object> propertiesMap();


  @Deprecated public final void setProperty(String key, Object value) {setPropertyImpl(key, value);};
  protected abstract void setPropertyImpl(String key, Object value);
  final void setPropertyInternal(String key, Object value) {setPropertyImpl(key, value);}

  @Deprecated public final <A> void setProperty(PropertyKey<A> key, A value){setPropertyImpl(key, value);}
  protected abstract <A> void setPropertyImpl(PropertyKey<A> key, A value);
  final <A> void setPropertyInternal(PropertyKey<A> key, A value){setPropertyImpl(key, value);}


  @Deprecated public final void setProperty(Property<?> property){setPropertyImpl(property);}
  protected abstract void setPropertyImpl(Property<?> property);
  final void setPropertyInternal(Property<?> property){setPropertyImpl(property);}


  @Deprecated final public void removeProperty(String key){removePropertyImpl(key);}
  protected abstract void removePropertyImpl(String key);
  final void removePropertyInternal(String key){removePropertyImpl(key);}


  @Deprecated public final void remove(){removeImpl();};
  protected abstract void removeImpl();
  final void removeInternal(){removeImpl();};

}
