package io.shiftleft.overflowdb;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class NodePropertiesIndex {

  protected Map<String, Map<Object, Set<NodeRef>>> index = new ConcurrentHashMap<>();
  private final Set<String> indexedKeys = new HashSet<>();
  private final OdbGraph graph;

  public NodePropertiesIndex(final OdbGraph graph) {
    this.graph = graph;
  }

  protected void put(final String key, final Object value, final NodeRef nodeRef) {
    Map<Object, Set<NodeRef>> keyMap = this.index.get(key);
    if (null == keyMap) {
      // TODO use concurrent but memory efficient map
      this.index.putIfAbsent(key, new ConcurrentHashMap<>());
      keyMap = this.index.get(key);
    }
    Set<NodeRef> objects = keyMap.get(value);
    if (null == objects) {
      keyMap.putIfAbsent(value, ConcurrentHashMap.newKeySet());
      objects = keyMap.get(value);
    }
    objects.add(nodeRef);
  }

  public List<NodeRef> get(final String key, final Object value) {
    final Map<Object, Set<NodeRef>> keyMap = this.index.get(key);
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

  public void remove(final String key, final Object value, final NodeRef nodeRef) {
    final Map<Object, Set<NodeRef>> keyMap = this.index.get(key);
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

  public void removeElement(final NodeRef nodeRef) {
    for (Map<Object, Set<NodeRef>> map : index.values()) {
      for (Set<NodeRef> set : map.values()) {
        set.remove(nodeRef);
      }
    }
  }

  public void update(final String key, final Object newValue, final NodeRef nodeRef) {
    if (this.indexedKeys.contains(key)) {
      this.put(key, newValue, nodeRef);
    }
  }

  public void createKeyIndex(final String key) {
    if (null == key)
      throw Graph.Exceptions.argumentCanNotBeNull("key");
    if (key.isEmpty())
      throw new IllegalArgumentException("The key for the index cannot be an empty string");

    if (this.indexedKeys.contains(key))
      return;
    this.indexedKeys.add(key);

    this.graph.nodes.valueCollection().parallelStream()
        .map(e -> new Object[]{e.property(key), e})
        .filter(a -> ((Property) a[0]).isPresent())
        .forEach(a -> this.put(key, ((Property) a[0]).value(), (NodeRef) a[1]));
  }

  public void dropKeyIndex(final String key) {
    if (this.index.containsKey(key))
      this.index.remove(key).clear();

    this.indexedKeys.remove(key);
  }

  public Set<String> getIndexedKeys() {
    return this.indexedKeys;
  }

}
