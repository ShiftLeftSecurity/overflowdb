package io.shiftleft.overflowdb;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class NodePropertiesIndex<T extends Element> {

  protected Map<String, Map<Object, Set<T>>> index = new ConcurrentHashMap<>();
  private final Set<String> indexedKeys = new HashSet<>();
  private final OdbGraph graph;

  public NodePropertiesIndex(final OdbGraph graph) {
    this.graph = graph;
  }

  protected void put(final String key, final Object value, final T element) {
    Map<Object, Set<T>> keyMap = this.index.get(key);
    if (null == keyMap) {
      // TODO use concurrent but memory efficient map
      this.index.putIfAbsent(key, new ConcurrentHashMap<>());
      keyMap = this.index.get(key);
    }
    Set<T> objects = keyMap.get(value);
    if (null == objects) {
      keyMap.putIfAbsent(value, ConcurrentHashMap.newKeySet());
      objects = keyMap.get(value);
    }
    objects.add(element);
  }

  public List<T> get(final String key, final Object value) {
    final Map<Object, Set<T>> keyMap = this.index.get(key);
    if (null == keyMap) {
      return Collections.emptyList();
    } else {
      Set<T> set = keyMap.get(value);
      if (null == set)
        return Collections.emptyList();
      else
        return new ArrayList<>(set);
    }
  }

  public void remove(final String key, final Object value, final T element) {
    final Map<Object, Set<T>> keyMap = this.index.get(key);
    if (null != keyMap) {
      Set<T> objects = keyMap.get(value);
      if (null != objects) {
        objects.remove(element);
        if (objects.size() == 0) {
          keyMap.remove(value);
        }
      }
    }
  }

  public void removeElement(final T element) {
    for (Map<Object, Set<T>> map : index.values()) {
      for (Set<T> set : map.values()) {
        set.remove(element);
      }
    }
  }

  public void autoUpdate(final String key, final Object newValue, final Object oldValue, final T element) {
    if (this.indexedKeys.contains(key)) {
      if (oldValue != null)
        this.remove(key, oldValue, element);
      this.put(key, newValue, element);
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

    this.graph.nodes.valueCollection().<T>parallelStream()
        .map(e -> new Object[]{((T) e).property(key), e})
        .filter(a -> ((Property) a[0]).isPresent())
        .forEach(a -> this.put(key, ((Property) a[0]).value(), (T) a[1]));
  }

  public void dropKeyIndex(final String key) {
    if (this.index.containsKey(key))
      this.index.remove(key).clear();

    this.indexedKeys.remove(key);
  }

  public Set<String> getIndexedKeys() {
    return this.indexedKeys;
  }

  public static List<Vertex> queryNodeIndex(final OdbGraph graph, final String key, final Object value) {
    return null == graph.nodeIndex ? Collections.emptyList() : graph.nodeIndex.get(key, value);
  }

  public static void autoUpdateIndex(final Vertex vertex, final String key, final Object newValue, final Object oldValue) {
    final OdbGraph graph = (OdbGraph) vertex.graph();
    if (graph.nodeIndex != null)
      graph.nodeIndex.autoUpdate(key, newValue, oldValue, vertex);
  }

  public static void removeElementIndex(final Vertex vertex) {
    final OdbGraph graph = (OdbGraph) vertex.graph();
    if (graph.nodeIndex != null)
      graph.nodeIndex.removeElement(vertex);
  }
}
