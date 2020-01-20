package io.shiftleft.overflowdb;

import org.apache.tinkerpop.gremlin.structure.Property;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class OdbIndexManager {

  private final OdbGraph graph;
  // TODO use concurrent but memory efficient map
  protected Map<String, Map<Object, Set<NodeRef>>> indexes = new ConcurrentHashMap<>();

  public OdbIndexManager(OdbGraph graph) {
    this.graph = graph;
  }

  /**
   * Create an index for specified node property.
   * Whenever an element has the specified key mutated, the index is updated.
   * When the index is created, all existing elements are indexed to ensure that they are captured by the index.
   */
  public final void createNodePropertyIndex(final String propertyName) {
    if (propertyName == null || propertyName.isEmpty())
      throw new IllegalArgumentException("Illegal property name: " + propertyName);

    if (indexes.containsKey(propertyName))
      return;

    graph.nodes.valueCollection().parallelStream()
        .map(e -> new Object[]{e.property(propertyName), e})
        .filter(a -> ((Property) a[0]).isPresent())
        .forEach(a -> put(propertyName, ((Property) a[0]).value(), (NodeRef) a[1]));
  }

  public void putIfIndexed(final String key, final Object newValue, final NodeRef nodeRef) {
    if (indexes.containsKey(key)) {
      put(key, newValue, nodeRef);
    }
  }

  private final void put(final String key, final Object value, final NodeRef nodeRef) {
    Map<Object, Set<NodeRef>> keyMap = indexes.get(key);
    if (null == keyMap) {
      indexes.putIfAbsent(key, new ConcurrentHashMap<>());
      keyMap = indexes.get(key);
    }
    Set<NodeRef> objects = keyMap.get(value);
    if (null == objects) {
      keyMap.putIfAbsent(value, ConcurrentHashMap.newKeySet());
      objects = keyMap.get(value);
    }
    objects.add(nodeRef);
  }

  /**
   * Drop the index for specified node property.
   */
  public final void dropNodePropertyIndex(final String key) {
    if (indexes.containsKey(key))
      indexes.remove(key).clear();
  }

  /**
   * Return all the keys currently being indexed for nodes.
   */
  public final Set<String> getIndexedNodeProperties() {
    return indexes.keySet();
  }

  public final List<NodeRef> lookup(final String key, final Object value) {
    final Map<Object, Set<NodeRef>> keyMap = indexes.get(key);
    if (null == keyMap) {
      return Collections.emptyList();
    } else {
      Set<NodeRef> set = keyMap.get(value);
      if (null == set)
        return Collections.emptyList();
      else
        return new ArrayList<>(set);
    }
  }

  public final void remove(final String key, final Object value, final NodeRef nodeRef) {
    final Map<Object, Set<NodeRef>> keyMap = indexes.get(key);
    if (null != keyMap) {
      Set<NodeRef> objects = keyMap.get(value);
      if (null != objects) {
        objects.remove(nodeRef);
        if (objects.size() == 0) {
          keyMap.remove(value);
        }
      }
    }
  }

  public final void removeElement(final NodeRef nodeRef) {
    for (Map<Object, Set<NodeRef>> map : indexes.values()) {
      for (Set<NodeRef> set : map.values()) {
        set.remove(nodeRef);
      }
    }
  }

}
