package overflowdb;

import overflowdb.storage.OdbStorage;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.h2.mvstore.MVMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.LongStream;

public final class OdbIndexManager {

  private final OdbGraph graph;
  // TODO use concurrent but memory efficient map
  protected Map<String, Map<Object, Set<NodeRef>>> indexes = new ConcurrentHashMap<>();
  protected Map<String, Boolean> dirtyFlags = new ConcurrentHashMap<>();

  public OdbIndexManager(OdbGraph graph) {
    this.graph = graph;
  }

  /**
   * Create an index for specified node property.
   * Whenever an element has the specified key mutated, the index is updated.
   * When the index is created, all existing elements are indexed to ensure that they are captured by the index.
   */
  public final void createNodePropertyIndex(final String propertyName) {
    checkPropertyName(propertyName);

    if (indexes.containsKey(propertyName))
      return;

    dirtyFlags.put(propertyName, true);

    graph.nodes.parallelStream()
        .map(e -> new Object[]{e.property(propertyName), e})
        .filter(a -> ((Property) a[0]).isPresent())
        .forEach(a -> put(propertyName, ((Property) a[0]).value(), (NodeRef) a[1]));
  }

  private void checkPropertyName(String propertyName) {
    if (propertyName == null || propertyName.isEmpty())
      throw new IllegalArgumentException("Illegal property name: " + propertyName);
  }

  public final void loadNodePropertyIndex(final String propertyName, Map<Object, long[]> valueToNodeIds) {
    dirtyFlags.put(propertyName, false);
    valueToNodeIds.entrySet().parallelStream().forEach(entry ->
        LongStream.of(entry.getValue())
          .forEach(nodeId -> put(propertyName, entry.getKey(), (NodeRef)graph.vertex(nodeId))));
  }

  public void putIfIndexed(final String key, final Object newValue, final NodeRef nodeRef) {
    dirtyFlags.put(key, true);
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
    if (indexes.containsKey(key)) {
      indexes.remove(key).clear();
      dirtyFlags.remove(key);
    }
  }

  /**
   * Return all the keys currently being indexed for nodes.
   */
  public final Set<String> getIndexedNodeProperties() {
    return indexes.keySet();
  }

  public final int getIndexedNodeCount(String propertyName) {
    final Map<Object, Set<NodeRef>> indexMap = this.indexes.get(propertyName);
    return indexMap == null ? 0 : indexMap.values().stream().mapToInt(Set::size).sum();
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
    dirtyFlags.put(key, true);
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
    for (String propertyName : indexes.keySet())
      dirtyFlags.put(propertyName, true);
    for (Map<Object, Set<NodeRef>> map : indexes.values()) {
      for (Set<NodeRef> set : map.values()) {
        set.remove(nodeRef);
      }
    }
  }

  public Map<Object, Set<NodeRef>> getIndexMap(String propertyName) {
    return this.indexes.get(propertyName);
  }

  public void initializeStoredIndices(OdbStorage storage) {
    storage
        .getIndexNames()
        .stream()
        .forEach(indexName -> loadIndex(indexName, storage));
  }

  public void loadIndex(String indexName, OdbStorage storage) {
    final MVMap<Object, long[]> indexMVMap = storage.openIndex(indexName);
    loadNodePropertyIndex(indexName, indexMVMap);
  }

  public void storeIndexes(OdbStorage storage) {
    getIndexedNodeProperties()
        .stream()
        .forEach(propertyName ->
            saveIndex(storage, propertyName, getIndexMap(propertyName)));
  }

  private void saveIndex(OdbStorage storage, String propertyName, Map<Object, Set<NodeRef>> indexMap) {
    if (dirtyFlags.get(propertyName)) {
      storage.clearIndex(propertyName);
      final MVMap<Object, long[]> indexStore = storage.openIndex(propertyName);
      indexMap.entrySet().parallelStream().forEach(entry -> {
        final Object propertyValue = entry.getKey();
        final Set<NodeRef> nodeRefs = entry.getValue();
        indexStore.put(propertyValue, nodeRefs.stream().mapToLong(nodeRef -> nodeRef.id).toArray());
      });
      dirtyFlags.put(propertyName, false);
    }
  }
}
