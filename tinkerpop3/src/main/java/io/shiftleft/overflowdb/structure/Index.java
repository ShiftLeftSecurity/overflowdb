package io.shiftleft.overflowdb.structure;

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

final class Index<T extends Element> {

  protected Map<String, Map<Object, Set<T>>> index = new ConcurrentHashMap<>();
  protected final Class<T> indexClass;
  private final Set<String> indexedKeys = new HashSet<>();
  private final OverflowDbGraph graph;

  public Index(final OverflowDbGraph graph, final Class<T> indexClass) {
    this.graph = graph;
    this.indexClass = indexClass;
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

  public long count(final String key, final Object value) {
    final Map<Object, Set<T>> keyMap = this.index.get(key);
    if (null == keyMap) {
      return 0;
    } else {
      Set<T> set = keyMap.get(value);
      if (null == set)
        return 0;
      else
        return set.size();
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
    if (this.indexClass.isAssignableFrom(element.getClass())) {
      for (Map<Object, Set<T>> map : index.values()) {
        for (Set<T> set : map.values()) {
          set.remove(element);
        }
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

  public void autoRemove(final String key, final Object oldValue, final T element) {
    if (this.indexedKeys.contains(key))
      this.remove(key, oldValue, element);
  }

  public void createKeyIndex(final String key) {
    if (null == key)
      throw Graph.Exceptions.argumentCanNotBeNull("key");
    if (key.isEmpty())
      throw new IllegalArgumentException("The key for the index cannot be an empty string");

    if (this.indexedKeys.contains(key))
      return;
    this.indexedKeys.add(key);

    if (Vertex.class.isAssignableFrom(this.indexClass)) {
      this.graph.nodes.valueCollection().<T>parallelStream()
          .map(e -> new Object[]{((T) e).property(key), e})
          .filter(a -> ((Property) a[0]).isPresent())
          .forEach(a -> this.put(key, ((Property) a[0]).value(), (T) a[1]));
    } else {
      throw new NotImplementedException("");
    }
  }

  public void dropKeyIndex(final String key) {
    if (this.index.containsKey(key))
      this.index.remove(key).clear();

    this.indexedKeys.remove(key);
  }

  public Set<String> getIndexedKeys() {
    return this.indexedKeys;
  }


//    public static List<Vertex> queryVertexIndex(final OverflowDb graph, final String key, final Object value) {
//        return null == graph.vertexIndex ? Collections.emptyList() : graph.vertexIndex.get(key, value);
//    }
//
//    public static List<Edge> queryEdgeIndex(final OverflowDb graph, final String key, final Object value) {
//        return Collections.emptyList();
//    }
//
//    public static Map<String, List<VertexProperty>> getProperties(final TinkerVertex vertex) {
//        return null == vertex.properties ? Collections.emptyMap() : vertex.properties;
//    }

//    public static void autoUpdateIndex(final Edge edge, final String key, final Object newValue, final Object oldValue) {
//        final OverflowDb graph = (OverflowDb) edge.graph();
//
//        if (graph.edgeIndex != null)
//            graph.edgeIndex.autoUpdate(key, newValue, oldValue, edge);
//    }

  public static void autoUpdateIndex(final Vertex vertex, final String key, final Object newValue, final Object oldValue) {
    final OverflowDbGraph graph = (OverflowDbGraph) vertex.graph();
    if (graph.nodeIndex != null)
      graph.nodeIndex.autoUpdate(key, newValue, oldValue, vertex);
  }

  public static void removeElementIndex(final Vertex vertex) {
    final OverflowDbGraph graph = (OverflowDbGraph) vertex.graph();
    if (graph.nodeIndex != null)
      graph.nodeIndex.removeElement(vertex);
  }

//    public static void removeElementIndex(final Edge edge) {
//        final OverflowDb graph = (OverflowDb) edge.graph();
//        if (graph.edgeIndex != null)
//            graph.edgeIndex.removeElement(edge);
//    }
//
//    public static void removeIndex(final TinkerVertex vertex, final String key, final Object value) {
//        final OverflowDb graph = (OverflowDb) vertex.graph();
//        if (graph.vertexIndex != null)
//            graph.vertexIndex.remove(key, value, vertex);
//    }

//    public static void removeIndex(final TinkerEdge edge, final String key, final Object value) {
//        final OverflowDb graph = (OverflowDb) edge.graph();
//        if (graph.edgeIndex != null)
//            graph.edgeIndex.remove(key, value, edge);
//    }
}
