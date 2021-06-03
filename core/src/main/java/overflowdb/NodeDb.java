package overflowdb;

import overflowdb.util.ArrayOffsetIterator;
import overflowdb.util.DummyEdgeIterator;
import overflowdb.util.MultiIterator;
import overflowdb.util.PackedIntArray;
import overflowdb.util.PropertyHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Holds node properties and edges to adjacent nodes (including edge properties).
 * Each {{@link NodeRef}} refers to exactly one NodeDb instance, and if required can set that instance to `null`, thus
 * freeing up memory, e.g. if heap memory is low. While {{@link NodeRef}} instances are very small (they will never be
 * garbage collected), NodeDb instances consume a bit more space.
 *
 * Adjacent nodes and edge properties are stored in a flat array (adjacentNodesWithEdgeProperties).
 * Edges only exist virtually and are created on request. This allows for a small memory footprint, especially given
 * that most graph domains have magnitudes more edges than nodes.
 */
public abstract class NodeDb implements Node {
  public final NodeRef ref;

  /**
   * holds refs to all adjacent nodes (a.k.a. dummy edges) and the edge properties
   */
  private Object[] adjacentNodesWithEdgeProperties = new Object[0];

  /* store the start offset and length into the above `adjacentNodesWithEdgeProperties` array in an interleaved manner,
   * i.e. each adjacent edge type has two entries in this array. */
  private PackedIntArray edgeOffsets;

  /**
   * Flag that helps us save time when serializing, both when overflowing to disk and when storing
   * the graph on close.
   * `true`  when node is first created, or is modified (property or edges)
   * `false` when node is freshly serialized to disk or deserialized from disk
   */
  private volatile boolean dirty;

  private static final String[] ALL_LABELS = new String[0];

  protected NodeDb(NodeRef ref) {
    this.ref = ref;

    ref.setNode(this);
    if (ref.graph != null) {
      ref.graph.referenceManager.applyBackpressureMaybe();
    }

    edgeOffsets = PackedIntArray.create(layoutInformation().numberOfDifferentAdjacentTypes() * 2);
  }

  public abstract NodeLayoutInformation layoutInformation();

  public Object[] getAdjacentNodesWithEdgeProperties() {
    return adjacentNodesWithEdgeProperties;
  }

  public void setAdjacentNodesWithEdgeProperties(Object[] adjacentNodesWithEdgeProperties) {
    this.adjacentNodesWithEdgeProperties = adjacentNodesWithEdgeProperties;
  }

  public int[] getEdgeOffsets() {
    return edgeOffsets.toIntArray();
  }

  public PackedIntArray getEdgeOffsetsPackedArray() {
    return edgeOffsets;
  }

  public void setEdgeOffsets(int[] edgeOffsets) {
    this.edgeOffsets = PackedIntArray.of(edgeOffsets);
  }

  public abstract Map<String, Object> valueMap();

  @Override
  public Graph graph() {
    return ref.graph;
  }

  public long id() {
    return ref.id;
  }

  @Override
  public String label() {
    return ref.label();
  }

  @Override
  public <A> A property(PropertyKey<A> key) {
    return (A) property(key.name);
  }

  @Override
  public <A> Optional<A> propertyOption(PropertyKey<A> key) {
    return Optional.ofNullable(property(key));
  }

  @Override
  public Optional<Object> propertyOption(String key) {
    return Optional.ofNullable(property(key));
  }

  @Override
  public Map<String, Object> propertyMap() {
    final Map<String, Object> results = new HashMap<>(propertyKeys().size());

    for (String propertyKey : propertyKeys()) {
      final Object value = property(propertyKey);
      if (value != null) results.put(propertyKey, value);
    }

    return results;
  }

  @Override
  public Set<String> propertyKeys() {
    return layoutInformation().propertyKeys();
  }

  @Override
  public void setProperty(String key, Object value) {
    updateSpecificProperty(key, value);
    ref.graph.indexManager.putIfIndexed(key, value, ref);
    /* marking as dirty *after* we updated - if node gets serialized before we finish, it'll be marked as dirty */
    this.markAsDirty();
  }

  @Override
  public <A> void setProperty(PropertyKey<A> key, A value) {
    setProperty(key.name, value);
  }

  @Override
  public void setProperty(Property<?> property) {
    setProperty(property.key.name, property.value);
  }

  @Override
  public void removeProperty(String key) {
    Object oldValue = property(key);
    removeSpecificProperty(key);
    ref.graph.indexManager.remove(key, oldValue, ref);
    /* marking as dirty *after* we updated - if node gets serialized before we finish, it'll be marked as dirty */
    this.markAsDirty();
  }

  protected abstract void updateSpecificProperty(String key, Object value);

  protected abstract void removeSpecificProperty(String key);

  @Override
  public void remove() {
    final List<Edge> edges = new ArrayList<>();
    bothE().forEachRemaining(edges::add);
    for (Edge edge : edges) {
      if (!edge.isRemoved()) {
        edge.remove();
      }
    }

    ref.graph.remove(this);

    /* marking as dirty *after* we updated - if node gets serialized before we finish, it'll be marked as dirty */
    this.markAsDirty();
  }

  public void markAsDirty() {
    this.dirty = true;
  }

  public void markAsClean() {
    this.dirty = false;
  }

  public <V> Iterator<V> getEdgeProperties(Direction direction,
                                                     Edge edge,
                                                     int blockOffset,
                                                     String... keys) {
    List<V> result = new ArrayList<>();

    if (keys.length != 0) {
      for (String key : keys) {
        result.add(edgeProperty(direction, edge, blockOffset, key));
      }
    } else {
      for (String propertyKey : layoutInformation().edgePropertyKeys(edge.label())) {
        result.add(edgeProperty(direction, edge, blockOffset, propertyKey));
      }
    }

    return result.iterator();
  }

  public Map<String, Object> edgePropertyMap(Direction direction, Edge edge, int blockOffset) {
    final Set<String> edgePropertyKeys = layoutInformation().edgePropertyKeys(edge.label());
    final Map<String, Object> results = new HashMap<>(edgePropertyKeys.size());

    for (String propertyKey : edgePropertyKeys) {
      final Object value = edgeProperty(direction, edge, blockOffset, propertyKey);
      if (value != null) results.put(propertyKey, value);
    }

    return results;
  }

  public <V> Optional<V> edgePropertyOption(Direction direction,
                                            Edge edge,
                                            int blockOffset,
                                            String key) {
    V value = edgeProperty(direction, edge, blockOffset, key);
    return Optional.ofNullable(value);
  }

  public <P> P edgeProperty(Direction direction,
                            Edge edge,
                            int blockOffset,
                            String key) {
    int propertyPosition = getEdgePropertyIndex(direction, edge.label(), key, blockOffset);
    if (propertyPosition == -1) {
      return null;
    }
    return (P) adjacentNodesWithEdgeProperties[propertyPosition];
  }

  public <V> void setEdgeProperty(Direction direction,
                                  String edgeLabel,
                                  String key,
                                  V value,
                                  int blockOffset) {
    int propertyPosition = getEdgePropertyIndex(direction, edgeLabel, key, blockOffset);
    if (propertyPosition == -1) {
      throw new RuntimeException("Edge " + edgeLabel + " does not support property `" + key + "`.");
    }
    adjacentNodesWithEdgeProperties[propertyPosition] = value;
    /* marking as dirty *after* we updated - if node gets serialized before we finish, it'll be marked as dirty */
    this.markAsDirty();
  }

  public void removeEdgeProperty(Direction direction, String edgeLabel, String key, int blockOffset) {
    int propertyPosition = getEdgePropertyIndex(direction, edgeLabel, key, blockOffset);
    if (propertyPosition == -1) {
      throw new RuntimeException("Edge " + edgeLabel + " does not support property `" + key + "`.");
    }
    adjacentNodesWithEdgeProperties[propertyPosition] = null;
    /* marking as dirty *after* we updated - if node gets serialized before we finish, it'll be marked as dirty */
    this.markAsDirty();
  }

  private int calcAdjacentNodeIndex(Direction direction,
                                    String edgeLabel,
                                    int blockOffset) {
    int offsetPos = getPositionInEdgeOffsets(direction, edgeLabel);
    if (offsetPos == -1) {
      return -1;
    }

    int start = startIndex(offsetPos);
    return start + blockOffset;
  }

  /**
   * Return -1 if there exists no edge property for the provided argument combination.
   */
  private int getEdgePropertyIndex(Direction direction,
                                   String label,
                                   String key,
                                   int blockOffset) {
    int adjacentNodeIndex = calcAdjacentNodeIndex(direction, label, blockOffset);
    if (adjacentNodeIndex == -1) {
      return -1;
    }

    int propertyOffset = layoutInformation().getEdgePropertyOffsetRelativeToAdjacentNodeRef(label, key);
    if (propertyOffset == -1) {
      return -1;
    }

    return adjacentNodeIndex + propertyOffset;
  }

  @Override
  public Edge addEdge(String label, Node inNode, Object... keyValues) {
    final NodeRef inNodeRef = (NodeRef) inNode;
    NodeRef thisNodeRef = ref;

    int outBlockOffset = storeAdjacentNode(Direction.OUT, label, inNodeRef, keyValues);
    int inBlockOffset = inNodeRef.get().storeAdjacentNode(Direction.IN, label, thisNodeRef, keyValues);

    Edge dummyEdge = instantiateDummyEdge(label, thisNodeRef, inNodeRef);
    dummyEdge.setOutBlockOffset(outBlockOffset);
    dummyEdge.setInBlockOffset(inBlockOffset);

    return dummyEdge;
  }

  @Override
  public Edge addEdge(String label, Node inNode, Map<String, Object> keyValues) {
    return addEdge(label, inNode, PropertyHelper.toKeyValueArray(keyValues));
  }

  @Override
  public void addEdgeSilent(String label, Node inNode, Object... keyValues) {
    final NodeRef inNodeRef = (NodeRef) inNode;
    NodeRef thisNodeRef = ref;

    storeAdjacentNode(Direction.OUT, label, inNodeRef, keyValues);
    inNodeRef.get().storeAdjacentNode(Direction.IN, label, thisNodeRef, keyValues);
  }

  @Override
  public void addEdgeSilent(String label, Node inNode, Map<String, Object> keyValues) {
    addEdgeSilent(label, inNode, PropertyHelper.toKeyValueArray(keyValues));
  }

  /* adjacent OUT nodes (all labels) */
  @Override
  public Iterator<Node> out() {
    return createAdjacentNodeIterator(Direction.OUT, ALL_LABELS);
  }

  /* adjacent OUT nodes for given labels */
  @Override
  public Iterator<Node> out(String... edgeLabels) {
    return createAdjacentNodeIterator(Direction.OUT, edgeLabels);
  }

  /* adjacent IN nodes (all labels) */
  @Override
  public Iterator<Node> in() {
    final MultiIterator<Node> multiIterator = new MultiIterator<>();
    for (String label : layoutInformation().allowedInEdgeLabels()) {
      multiIterator.addIterator(in(label));
    }
    return multiIterator;
  }

  /* adjacent IN nodes for given labels */
  @Override
  public Iterator<Node> in(String... edgeLabels) {
    return createAdjacentNodeIterator(Direction.IN, edgeLabels);
  }

  /* adjacent OUT/IN nodes (all labels) */
  @Override
  public Iterator<Node> both() {
    final MultiIterator<Node> multiIterator = new MultiIterator<>();
    multiIterator.addIterator(out());
    multiIterator.addIterator(in());
    return multiIterator;
  }

  /* adjacent OUT/IN nodes for given labels */
  @Override
  public Iterator<Node> both(String... edgeLabels) {
    final MultiIterator<Node> multiIterator = new MultiIterator<>();
    multiIterator.addIterator(out(edgeLabels));
    multiIterator.addIterator(in(edgeLabels));
    return multiIterator;
  }

  /* adjacent OUT edges (all labels) */
  @Override
  public Iterator<Edge> outE() {
    final MultiIterator<Edge> multiIterator = new MultiIterator<>();
    for (String label : layoutInformation().allowedOutEdgeLabels()) {
      multiIterator.addIterator(outE(label));
    }
    return multiIterator;
  }

  /* adjacent OUT edges for given labels */
  @Override
  public Iterator<Edge> outE(String... edgeLabels) {
    return createDummyEdgeIterator(Direction.OUT, edgeLabels);
  }

  /* adjacent IN edges (all labels) */
  @Override
  public Iterator<Edge> inE() {
    final MultiIterator<Edge> multiIterator = new MultiIterator<>();
    for (String label : layoutInformation().allowedInEdgeLabels()) {
      multiIterator.addIterator(inE(label));
    }
    return multiIterator;
  }

  /* adjacent IN edges for given labels */
  @Override
  public Iterator<Edge> inE(String... edgeLabels) {
    return createDummyEdgeIterator(Direction.IN, edgeLabels);
  }

  /* adjacent OUT/IN edges (all labels) */
  @Override
  public Iterator<Edge> bothE() {
    final MultiIterator<Edge> multiIterator = new MultiIterator<>();
    multiIterator.addIterator(outE());
    multiIterator.addIterator(inE());
    return multiIterator;
  }

  /* adjacent OUT/IN edges for given labels */
  @Override
  public Iterator<Edge> bothE(String... edgeLabels) {
    final MultiIterator<Edge> multiIterator = new MultiIterator<>();
    multiIterator.addIterator(outE(edgeLabels));
    multiIterator.addIterator(inE(edgeLabels));
    return multiIterator;
  }

  protected int outEdgeCount() {
    int count = 0;
    for (String label : layoutInformation().allowedOutEdgeLabels()) {
      int offsetPos = getPositionInEdgeOffsets(Direction.OUT, label);
      if (offsetPos != -1) {
        int start = startIndex(offsetPos);
        int length = blockLength(offsetPos);
        int strideSize = getStrideSize(label);
        int exclusiveEnd = start + length;
        for (int i = start;
             i < adjacentNodesWithEdgeProperties.length && i < exclusiveEnd;
             i += strideSize) {
          if (adjacentNodesWithEdgeProperties[i] != null) {
            count++;
          }
        }
      }
    }
    return count;
  }

  /**
   * If there are multiple edges between the same two nodes with the same label, we use the
   * `occurrence` to differentiate between those edges. Both nodes use the same occurrence
   * index for the same edge.
   *
   * @return the occurrence for a given edge, calculated by counting the number times the given
   * adjacent node occurred between the start of the edge-specific block and the blockOffset
   */
  protected final int blockOffsetToOccurrence(Direction direction,
                                     String label,
                                     NodeRef otherNode,
                                     int blockOffset) {
    int offsetPos = getPositionInEdgeOffsets(direction, label);
    int start = startIndex(offsetPos);
    int strideSize = getStrideSize(label);

    int occurrenceCount = -1;
    for (int i = start; i <= start + blockOffset; i += strideSize) {
      final NodeRef adjacentNodeWithProperty = (NodeRef) adjacentNodesWithEdgeProperties[i];
      if (adjacentNodeWithProperty != null &&
          adjacentNodeWithProperty.id() == otherNode.id()) {
        occurrenceCount++;
      }
    }

    if (occurrenceCount == -1)
      throw new RuntimeException("unable to calculate occurrenceCount");
    else
      return occurrenceCount;
  }

  /**
   * @param direction  OUT or IN
   * @param label      the edge label
   * @param occurrence if there are multiple edges between the same two nodes with the same label,
   *                   this is used to differentiate between those edges.
   *                   Both nodes use the same occurrence index in their `adjacentNodesWithEdgeProperties` array for the same edge.
   * @return the index into `adjacentNodesWithEdgeProperties`
   */
  protected final int occurrenceToBlockOffset(Direction direction,
                                     String label,
                                     NodeRef adjacentNode,
                                     int occurrence) {
    int offsetPos = getPositionInEdgeOffsets(direction, label);
    int start = startIndex(offsetPos);
    int length = blockLength(offsetPos);
    int strideSize = getStrideSize(label);

    int currentOccurrence = 0;
    int exclusiveEnd = start + length;
    for (int i = start; i < exclusiveEnd; i += strideSize) {
      final NodeRef adjacentNodeWithProperty = (NodeRef) adjacentNodesWithEdgeProperties[i];
      if (adjacentNodeWithProperty != null &&
          adjacentNodeWithProperty.id() == adjacentNode.id()) {
        if (currentOccurrence == occurrence) {
          int adjacentNodeIndex = i - start;
          return adjacentNodeIndex;
        } else {
          currentOccurrence++;
        }
      }
    }
    throw new RuntimeException("Unable to find occurrence " + occurrence + " of "
        + label + " edge to node " + adjacentNode.id());
  }

  /**
   * Removes an 'edge', i.e. in reality it removes the information about the adjacent node from
   * `adjacentNodesWithEdgeProperties`. The corresponding elements will be set to `null`, i.e. we'll have holes.
   * Note: this decrements the `offset` of the following edges in the same block by one, but that's ok because the only
   * thing that matters is that the offset is identical for both connected nodes (assuming thread safety).
   *
   * @param blockOffset must have been initialized
   */
  protected final synchronized void removeEdge(Direction direction, String label, int blockOffset) {
    int offsetPos = getPositionInEdgeOffsets(direction, label);
    int start = startIndex(offsetPos) + blockOffset;
    int strideSize = getStrideSize(label);

    for (int i = start; i < start + strideSize; i++) {
      adjacentNodesWithEdgeProperties[i] = null;
    }

    /* marking as dirty *after* we updated - if node gets serialized before we finish, it'll be marked as dirty */
    this.markAsDirty();
  }

  private Iterator<Edge> createDummyEdgeIterator(Direction direction, String... labels) {
    if (labels.length == 1) {
      return createDummyEdgeIteratorForSingleLabel(direction, labels[0]);
    } else {
      final String[] labelsToFollow =
          labels.length == 0
              ? allowedLabelsByDirection(direction)
              : labels;
      final MultiIterator<Edge> multiIterator = new MultiIterator<>();
      for (String label : labelsToFollow) {
        multiIterator.addIterator(createDummyEdgeIteratorForSingleLabel(direction, label));
      }
      return multiIterator;
    }
  }

  private Iterator<Edge> createDummyEdgeIteratorForSingleLabel(Direction direction, String label) {
    int offsetPos = getPositionInEdgeOffsets(direction, label);
    if (offsetPos != -1) {
      int start = startIndex(offsetPos);
      int length = blockLength(offsetPos);
      int strideSize = getStrideSize(label);

      return new DummyEdgeIterator(adjacentNodesWithEdgeProperties, start, start + length, strideSize,
          direction, label, ref);
    } else {
      return Collections.emptyIterator();
    }
  }

  private final <A extends Node> Iterator<A> createAdjacentNodeIterator(Direction direction, String... labels) {
    if (labels.length == 1) {
      return createAdjacentNodeIteratorByOffSet(getPositionInEdgeOffsets(direction, labels[0]));
    } else {
      final String[] labelsToFollow =
          labels.length == 0
              ? allowedLabelsByDirection(direction)
              : labels;
      final MultiIterator<A> multiIterator = new MultiIterator<>();
      for (String label : labelsToFollow) {
        multiIterator.addIterator(createAdjacentNodeIteratorByOffSet(getPositionInEdgeOffsets(direction, label)));
      }
      return multiIterator;
    }
  }

  /* Simplify hoisting of string lookups.
   * n.b. `final` so that the JIT compiler can inline it */
  public final <A extends Node> Iterator<A> createAdjacentNodeIteratorByOffSet(int offsetPos) {
    if (offsetPos != -1) {
      int start = startIndex(offsetPos);
      int length = blockLength(offsetPos);
      int strideSize = layoutInformation().getEdgePropertyCountByOffsetPos(offsetPos) + 1;
      return new ArrayOffsetIterator<>(adjacentNodesWithEdgeProperties, start, start + length, strideSize);
    } else {
      return Collections.emptyIterator();
    }
  }

  private final String[] allowedLabelsByDirection(Direction direction) {
    if (direction.equals(Direction.OUT))
      return layoutInformation().allowedOutEdgeLabels();
    else if (direction.equals(Direction.IN))
      return layoutInformation().allowedInEdgeLabels();
    else throw new UnsupportedOperationException(direction.toString());
  }

  public int storeAdjacentNode(Direction direction,
                                String edgeLabel,
                                NodeRef adjacentNode,
                                Object... edgeKeyValues) {
    int blockOffset = storeAdjacentNode(direction, edgeLabel, adjacentNode);

    /* set edge properties */
    for (int i = 0; i < edgeKeyValues.length; i = i + 2) {
      String key = (String) edgeKeyValues[i];
      Object value = edgeKeyValues[i + 1];
      setEdgeProperty(direction, edgeLabel, key, value, blockOffset);
    }

    /* marking as dirty *after* we updated - if node gets serialized before we finish, it'll be marked as dirty */
    this.markAsDirty();

    return blockOffset;
  }

  private final synchronized int storeAdjacentNode(Direction direction, String edgeLabel, NodeRef nodeRef) {
    int offsetPos = getPositionInEdgeOffsets(direction, edgeLabel);
    if (offsetPos == -1) {
      throw new RuntimeException("Edge of type " + edgeLabel + " with direction " + direction +
          " not supported by class " + getClass().getSimpleName());
    }
    int start = startIndex(offsetPos);
    int length = blockLength(offsetPos);
    int strideSize = getStrideSize(edgeLabel);

    int insertAt = start + length;
    if (adjacentNodesWithEdgeProperties.length <= insertAt
            || adjacentNodesWithEdgeProperties[insertAt] != null
            || (offsetPos + 1 < (edgeOffsets.length()>>1) && insertAt >= startIndex(offsetPos + 1))) {
      // space already occupied - grow adjacentNodesWithEdgeProperties array, leaving some room for more elements
      adjacentNodesWithEdgeProperties = growAdjacentNodesWithEdgeProperties(offsetPos, strideSize, insertAt, length);
    }

    adjacentNodesWithEdgeProperties[insertAt] = nodeRef;
    // update edgeOffset length to include the newly inserted element
    edgeOffsets.set(2 * offsetPos + 1, length + strideSize);

    int blockOffset = length;
    return blockOffset;
  }

  public int startIndex(int offsetPosition) {
    return edgeOffsets.get(2 * offsetPosition);
  }

  /**
   * @return number of elements reserved in `adjacentNodesWithEdgeProperties` for a given edge label
   * includes space for the node ref and all properties
   */
  public final int getStrideSize(String edgeLabel) {
    int sizeForNodeRef = 1;
    Set<String> allowedPropertyKeys = layoutInformation().edgePropertyKeys(edgeLabel);
    return sizeForNodeRef + allowedPropertyKeys.size();
  }

  /**
   * @return The position in edgeOffsets array. -1 if the edge label is not supported
   */
  private final int getPositionInEdgeOffsets(Direction direction, String label) {
    final Integer positionOrNull;
    if (direction == Direction.OUT) {
      positionOrNull = layoutInformation().outEdgeToOffsetPosition(label);
    } else {
      positionOrNull = layoutInformation().inEdgeToOffsetPosition(label);
    }
    if (positionOrNull != null) {
      return positionOrNull;
    } else {
      return -1;
    }
  }

  /**
   * Returns the length of an edge type block in the adjacentNodesWithEdgeProperties array.
   * Length means number of index positions.
   */
  public final int blockLength(int offsetPosition) {
    return edgeOffsets.get(2 * offsetPosition + 1);
  }

  /**
   * grow the adjacentNodesWithEdgeProperties array
   * <p>
   * preallocates more space than immediately necessary, so we don't need to grow the array every time
   * (tradeoff between performance and memory).
   * grows with the square root of the double of the current capacity.
   */
  private final synchronized Object[] growAdjacentNodesWithEdgeProperties(int offsetPos,
                                                   int strideSize,
                                                   int insertAt,
                                                   int currentLength) {
    // TODO optimize growth function - optimizing has potential to save a lot of memory, but the below slowed down processing massively
//    int currentCapacity = currentLength / strideSize;
//    double additionalCapacity = Math.sqrt(currentCapacity) + 1;
//    int additionalCapacityInt = (int) Math.ceil(additionalCapacity);
//    int additionalEntriesCount = additionalCapacityInt * strideSize;
    int growthEmptyFactor = 2;
    int additionalEntriesCount = (currentLength + strideSize) * growthEmptyFactor;
    int newSize = adjacentNodesWithEdgeProperties.length + additionalEntriesCount;
    Object[] newArray = new Object[newSize];
    System.arraycopy(adjacentNodesWithEdgeProperties, 0, newArray, 0, insertAt);
    System.arraycopy(adjacentNodesWithEdgeProperties, insertAt, newArray, insertAt + additionalEntriesCount, adjacentNodesWithEdgeProperties.length - insertAt);

    // Increment all following start offsets by `additionalEntriesCount`.
    for (int i = offsetPos + 1; 2 * i < edgeOffsets.length(); i++) {
      edgeOffsets.set(2 * i, edgeOffsets.get(2 * i) + additionalEntriesCount);
    }
    return newArray;
  }

  /**
   * to follow the tinkerpop api, instantiate and return a dummy edge, which doesn't really exist in the graph
   */
  public final Edge instantiateDummyEdge(String label, NodeRef outNode, NodeRef inNode) {
    final EdgeFactory edgeFactory = ref.graph.edgeFactoryByLabel.get(label);
    if (edgeFactory == null)
      throw new IllegalArgumentException("specializedEdgeFactory for label=" + label + " not found - please register on startup!");
    return edgeFactory.createEdge(ref.graph, outNode, inNode);
  }

  /**
   * Trims the node to save storage: shrinks overallocations
   * */
  public synchronized long trim(){
    int newSize = 0;
    for(int offsetPos = 0; 2*offsetPos < edgeOffsets.length(); offsetPos++){
      int length = blockLength(offsetPos);
      newSize += length;
    }
    Object[] newArray = new Object[newSize];

    int off = 0;
    for(int offsetPos = 0; 2*offsetPos < edgeOffsets.length(); offsetPos++){
      int start = startIndex(offsetPos);
      int length = blockLength(offsetPos);
      System.arraycopy(adjacentNodesWithEdgeProperties, start, newArray, off, length);
      edgeOffsets.set(2 * offsetPos, off);
      off += length;
    }
    int oldsize = adjacentNodesWithEdgeProperties.length;
    adjacentNodesWithEdgeProperties = newArray;
    return (long)newSize + ( ((long)oldsize) << 32);
  }

  public final boolean isDirty() {
    return dirty;
  }

  @Override
  public int hashCode() {
    /* NodeRef compares by id. We need the hash computation to be fast and allocation-free; but we don't need it
     * very strong. Plain java would use id ^ (id>>>32) ; we do a little bit of mixing.
     * The style (shift-xor 33 and multiply) is similar to murmur3; the multiply constant is randomly chosen odd number.
     * Feel free to change this.
     * */
    long tmp = (id() ^ (id() >>> 33)) * 0x1ca213a8d7b7d9b1L;
    return ((int) tmp) ^ ((int) (tmp >>> 32));
  }

  @Override
  public boolean equals(final Object obj) {
    return (this == obj) || ( (obj instanceof Node) && id() == ((Node) obj).id() );
  }
}
