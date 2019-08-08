package io.shiftleft.overflowdb;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;

import java.util.NoSuchElementException;

public class OdbProperty<V> implements Property<V> {
  private final String key;
  private final V value;
  private final Element element; //set to null

  public OdbProperty(String key, V value, Element element) {
    this.key = key;
    this.value = value;
    this.element = element;
  }

  @Override
  public String key() {
    return key;
  }

  @Override
  public V value() throws NoSuchElementException {
    return value;
  }

  @Override
  public boolean isPresent() {
    return true;
  }

  @Override
  public Element element() {
    return element;
  }

  @Override
  public void remove() {
    throw new RuntimeException("Not supported.");
  }
}
