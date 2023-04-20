package overflowdb;

import overflowdb.util.ArrayOffsetIterator;
import overflowdb.util.DummyEdgeIterator;
import overflowdb.util.MultiIterator;
import overflowdb.util.PropertyHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
 *
 * All write operations are synchronized using `synchronized(this)`, in order to avoid race conditions when updating the adjacent nodes.
 * Read operations are not locked, i.e. they are fast because they do not wait, but they may read outdated data.
 */
public abstract class NodeDb extends Node {
  public final NodeRef ref;

  /**
   * Using separate volatile container for the large array (adjacentNodesWithEdgeProperties) and the small
   * array (edgeOffsets) to prevent jit/cpu reordering, potentially leading to race conditions when edges are
   * added/removed and traversed by other threads in parallel.
   */
  private volatile AdjacentNodes adjacentNodes;

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
      ref.graph.applyBackpressureMaybe();
    }

    adjacentNodes = new AdjacentNodes(layoutInformation().numberOfDifferentAdjacentTypes());
  }

  public abstract NodeLayoutInformation layoutInformation();
  /**
   * Gets the adjacent nodes with properties, in internal packed format.
   * This function is really package-private, and only formally public to simplify internal organization of overflowdb.
   * */
  public AdjacentNodes getAdjacentNodes() {
    return adjacentNodes;
  }

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
  public Map<String, Object> propertiesMap() {
    final Map<String, Object> results = new HashMap<>(propertyKeys().size());

    for (String propertyKey : propertyKeys()) {
      final Object value = property(propertyKey);
      if (value != null) results.put(propertyKey, value);
    }

    return results;
  }

  /** All properties *but* the default values, to ensure we don't serialize those.
   * Providing a default implementation here, but the codegen overrides this for efficiency.
   * Properties may have different runtime types here than what they have in `properties()`, e.g. if the domain
   * classes use primitive arrays for efficiency.
   *  */
  public Map<String, Object> propertiesMapForStorage() {
    final Map<String, Object> results = new HashMap<>(propertyKeys().size());

    for (String propertyKey : propertyKeys()) {
      final Object value = property(propertyKey);
      if (value != null && !value.equals(propertyDefaultValue(propertyKey))) results.put(propertyKey, value);
    }

    return results;
  }

  @Override
  public Set<String> propertyKeys() {
    return layoutInformation().propertyKeys();
  }

  @Override
  public Object propertyDefaultValue(String propertyKey) {
    return ref.propertyDefaultValue(propertyKey);
  }

  @Override
  protected void setPropertyImpl(String key, Object value) {
    updateSpecificProperty(key, value);
    ref.graph.indexManager.putIfIndexed(key, value, ref);
    /* marking as dirty *after* we updated - if node gets serialized before we finish, it'll be marked as dirty */
    this.markAsDirty();
  }

  @Override
  protected <A> void setPropertyImpl(PropertyKey<A> key, A value) {
    setProperty(key.name, value);
  }

  @Override
  protected void setPropertyImpl(Property<?> property) {
    setProperty(property.key.name, property.value);
  }

  @Override
  protected void removePropertyImpl(String key) {
    Object oldValue = property(key);
    removeSpecificProperty(key);
    ref.graph.indexManager.remove(key, oldValue, ref);
    /* marking as dirty *after* we updated - if node gets serialized before we finish, it'll be marked as dirty */
    this.markAsDirty();
  }

  protected abstract void updateSpecificProperty(String key, Object value);

  protected abstract void removeSpecificProperty(String key);

  @Override
  protected void removeImpl() {
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

  /** returns a Map of all explicitly set properties of an edge,
   * i.e. does not contain the properties which have the default value */
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
    AdjacentNodes adjacentNodesTmp = this.adjacentNodes;
    int propertyPosition = getEdgePropertyIndex(adjacentNodesTmp, direction, edge.label(), key, blockOffset);
    if (propertyPosition == -1) {
      return null;
    }
    return (P) adjacentNodesTmp.nodesWithEdgeProperties[propertyPosition];
  }

  public synchronized <V> void setEdgeProperty(Direction direction,
                                  String edgeLabel,
                                  String key,
                                  V value,
                                  int blockOffset) {
    AdjacentNodes adjacentNodesTmp = this.adjacentNodes;
    int propertyPosition = getEdgePropertyIndex(adjacentNodesTmp, direction, edgeLabel, key, blockOffset);
    if (propertyPosition == -1) {
      throw new RuntimeException("Edge " + edgeLabel + " does not support property `" + key + "`.");
    }
    adjacentNodesTmp.nodesWithEdgeProperties[propertyPosition] = value;
    /* marking as dirty *after* we updated - if node gets serialized before we finish, it'll be marked as dirty */
    this.markAsDirty();
  }

  public void removeEdgeProperty(Direction direction, String edgeLabel, String key, int blockOffset) {
    setEdgeProperty(direction, edgeLabel, key, null, blockOffset);
  }

  private int calcAdjacentNodeIndex(AdjacentNodes adjacentNodesTmp,
                                    Direction direction,
                                    String edgeLabel,
                                    int blockOffset) {
    int offsetPos = getPositionInEdgeOffsets(direction, edgeLabel);
    if (offsetPos == -1) {
      return -1;
    }

    int start = startIndex(adjacentNodesTmp, offsetPos);
    return start + blockOffset;
  }

  /**
   * Return -1 if there exists no edge property for the provided argument combination.
   */
  private int getEdgePropertyIndex(AdjacentNodes adjacentNodesTmp,
                                   Direction direction,
                                   String label,
                                   String key,
                                   int blockOffset) {
    int adjacentNodeIndex = calcAdjacentNodeIndex(adjacentNodesTmp, direction, label, blockOffset);
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
  protected Edge addEdgeImpl(String label, Node inNode, Object... keyValues) {
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
  protected Edge addEdgeImpl(String label, Node inNode, Map<String, Object> keyValues) {
    return addEdge(label, inNode, PropertyHelper.toKeyValueArray(keyValues));
  }

  @Override
  protected void addEdgeSilentImpl(String label, Node inNode, Object... keyValues) {
    final NodeRef inNodeRef = (NodeRef) inNode;
    NodeRef thisNodeRef = ref;

    storeAdjacentNode(Direction.OUT, label, inNodeRef, keyValues);
    inNodeRef.get().storeAdjacentNode(Direction.IN, label, thisNodeRef, keyValues);
  }

  @Override
  protected void addEdgeSilentImpl(String label, Node inNode, Map<String, Object> keyValues) {
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
    AdjacentNodes adjacentNodesTmp = this.adjacentNodes;
    for (String label : layoutInformation().allowedOutEdgeLabels()) {
      int offsetPos = getPositionInEdgeOffsets(Direction.OUT, label);
      if (offsetPos != -1) {
        int start = startIndex(adjacentNodesTmp, offsetPos);
        int length = blockLength(adjacentNodesTmp, offsetPos);
        int strideSize = getStrideSize(label);
        int exclusiveEnd = start + length;
        for (int i = start;
             i < adjacentNodesTmp.nodesWithEdgeProperties.length && i < exclusiveEnd;
             i += strideSize) {
          if (adjacentNodesTmp.nodesWithEdgeProperties[i] != null) {
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
    AdjacentNodes adjacentNodesTmp = this.adjacentNodes;
    int offsetPos = getPositionInEdgeOffsets(direction, label);
    int start = startIndex(adjacentNodesTmp, offsetPos);
    int strideSize = getStrideSize(label);
    Object[] adjacentNodesWithEdgeProperties = adjacentNodesTmp.nodesWithEdgeProperties;

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
    AdjacentNodes adjacentNodesTmp = this.adjacentNodes;
    int offsetPos = getPositionInEdgeOffsets(direction, label);
    int start = startIndex(adjacentNodesTmp, offsetPos);
    int length = blockLength(adjacentNodesTmp, offsetPos);
    int strideSize = getStrideSize(label);

    Object[] adjacentNodesWithEdgeProperties = adjacentNodesTmp.nodesWithEdgeProperties;
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
    AdjacentNodes adjacentNodesTmp = this.adjacentNodes;
    int offsetPos = getPositionInEdgeOffsets(direction, label);
    int start = startIndex(adjacentNodesTmp, offsetPos) + blockOffset;
    int strideSize = getStrideSize(label);
    Object[] adjacentNodesWithEdgeProperties = adjacentNodesTmp.nodesWithEdgeProperties;

    for (int i = start; i < start + strideSize; i++) {
      adjacentNodesWithEdgeProperties[i] = null;
    }

    /* marking as dirty *after* we updated - if node gets serialized before we finish, it'll be marked as dirty */
    this.markAsDirty();
  }

  private Iterator<Edge> createDummyEdgeIterator(Direction direction, String... labels) {
    if (labels.length == 1) {
      return createDummyEdgeIteratorForSingleLabel(adjacentNodes, direction, labels[0]);
    } else {
      final String[] labelsToFollow =
          labels.length == 0
              ? allowedLabelsByDirection(direction)
              : labels;
      final MultiIterator<Edge> multiIterator = new MultiIterator<>();
      for (String label : labelsToFollow) {
        multiIterator.addIterator(createDummyEdgeIteratorForSingleLabel(adjacentNodes, direction, label));
      }
      return multiIterator;
    }
  }

  private Iterator<Edge> createDummyEdgeIteratorForSingleLabel(
      AdjacentNodes adjacentNodesTmp, Direction direction, String label) {
    int offsetPos = getPositionInEdgeOffsets(direction, label);
    if (offsetPos != -1) {
      int start = startIndex(adjacentNodesTmp, offsetPos);
      int length = blockLength(adjacentNodesTmp, offsetPos);
      int strideSize = getStrideSize(label);

      return new DummyEdgeIterator(
          adjacentNodesTmp.nodesWithEdgeProperties, start, start + length, strideSize, direction, label, ref);
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
    AdjacentNodes adjacentNodesTmp = this.adjacentNodes;
    if (offsetPos != -1) {
      int start = startIndex(adjacentNodesTmp, offsetPos);
      int length = blockLength(adjacentNodesTmp, offsetPos);
      int strideSize = layoutInformation().getEdgePropertyCountByOffsetPos(offsetPos) + 1;
      return new ArrayOffsetIterator<>(adjacentNodesTmp.nodesWithEdgeProperties, start, start + length, strideSize);
    } else {
      return Collections.emptyIterator();
    }
  }

  /* Simplify hoisting of string lookups.
   * n.b. `final` so that the JIT compiler can inline it */
  public final <A extends Node> scala.collection.Iterator<A> createAdjacentNodeScalaIteratorByOffSet(int offsetPos) {
    AdjacentNodes adjacentNodesTmp = this.adjacentNodes;
    if (offsetPos != -1) {
      int start = startIndex(adjacentNodesTmp, offsetPos);
      int length = blockLength(adjacentNodesTmp, offsetPos);
      int strideSize = layoutInformation().getEdgePropertyCountByOffsetPos(offsetPos) + 1;
      return new overflowdb.misc.ArrayIter<A>(adjacentNodesTmp.nodesWithEdgeProperties, start, start + length, strideSize);
    } else {
      return scala.collection.Iterator.empty();
    }
  }

  private final String[] allowedLabelsByDirection(Direction direction) {
    if (direction.equals(Direction.OUT))
      return layoutInformation().allowedOutEdgeLabels();
    else if (direction.equals(Direction.IN))
      return layoutInformation().allowedInEdgeLabels();
    else throw new UnsupportedOperationException(direction.toString());
  }

  public synchronized int storeAdjacentNode(Direction direction,
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

  //implicitly synchronized -- caller already holds monitor
  private final int storeAdjacentNode(Direction direction, String edgeLabel, NodeRef nodeRef) {
    AdjacentNodes tmp = this.adjacentNodes; //load acquire
    int offsetPos = getPositionInEdgeOffsets(direction, edgeLabel);
    if (offsetPos == -1) {
      throw new RuntimeException(
          String.format("Edge with type='%s' with direction='%s' not supported by nodeType='%s'" , edgeLabel, direction, label()));
    }
    int start = startIndex(tmp, offsetPos);
    int length = blockLength(tmp, offsetPos);
    int strideSize = getStrideSize(edgeLabel);

    Object[] adjacentNodesWithEdgeProperties = tmp.nodesWithEdgeProperties;
    int edgeOffsetLengthB2 = tmp.offsetLengths() >> 1;

    int insertAt = start + length;
    if (adjacentNodesWithEdgeProperties.length <= insertAt
        || adjacentNodesWithEdgeProperties[insertAt] != null
        || (offsetPos + 1 < edgeOffsetLengthB2 && insertAt >= startIndex(tmp, offsetPos + 1))) {
      // space already occupied - grow adjacentNodesWithEdgeProperties array, leaving some room for more elements
      tmp = growAdjacentNodesWithEdgeProperties(tmp, offsetPos, strideSize, insertAt, length);
    }

    tmp.nodesWithEdgeProperties[insertAt] = nodeRef;
    // update edgeOffset length to include the newly inserted element
    tmp = tmp.setOffset(2 * offsetPos + 1, length + strideSize);

    this.adjacentNodes = tmp; //store release
    int blockOffset = length;
    return blockOffset;
  }

  public int startIndex(AdjacentNodes adjacentNodesTmp, int offsetPosition) {
    return adjacentNodesTmp.getOffset(2 * offsetPosition);
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
  public final int blockLength(AdjacentNodes adjacentNodesTmp, int offsetPosition) {
    return adjacentNodesTmp.getOffset(2 * offsetPosition + 1);
  }

  /**
   * grow the adjacentNodesWithEdgeProperties array
   * <p>
   * preallocates more space than immediately necessary, so we don't need to grow the array every time
   * (tradeoff between performance and memory).
   * grows with the square root of the double of the current capacity.
   */
  private final AdjacentNodes growAdjacentNodesWithEdgeProperties(
      AdjacentNodes adjacentNodesOld, int offsetPos, int strideSize, int insertAt, int currentLength) {
    int growthEmptyFactor = 2;
    int additionalEntriesCount = (currentLength + strideSize) * growthEmptyFactor;
    Object[] nodesWithEdgePropertiesOld = adjacentNodesOld.nodesWithEdgeProperties;
    int newSize = nodesWithEdgePropertiesOld.length + additionalEntriesCount;
    Object[] nodesWithEdgePropertiesNew = new Object[newSize];
    System.arraycopy(nodesWithEdgePropertiesOld, 0, nodesWithEdgePropertiesNew, 0, insertAt);
    System.arraycopy(nodesWithEdgePropertiesOld, insertAt, nodesWithEdgePropertiesNew, insertAt + additionalEntriesCount, nodesWithEdgePropertiesOld.length - insertAt);
    AdjacentNodes res = new AdjacentNodes(nodesWithEdgePropertiesNew, adjacentNodesOld.offsets);
    // Increment all following start offsets by `additionalEntriesCount`.
    int until = res.offsetLengths();
    for (int i = offsetPos + 1; 2 * i < until; i++) {
      res = res.setOffset(2 * i, res.getOffset(2 * i) + additionalEntriesCount);
    }
    return res;
  }

  /**
   * instantiate and return a dummy edge, which doesn't really exist in the graph
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
  public synchronized long trim() {
    AdjacentNodes adjacentNodesOld = this.adjacentNodes;
    int newSize = 0;
    int until = adjacentNodesOld.offsetLengths();
    for (int offsetPos = 0; 2 * offsetPos < until; offsetPos++) {
      int length = blockLength(adjacentNodesOld, offsetPos);
      newSize += length;
    }
    Object[] nodesWithEdgePropertiesNew = new Object[newSize];
    AdjacentNodes res = new AdjacentNodes(nodesWithEdgePropertiesNew, new byte[until]);

    int off = 0;
    for(int offsetPos = 0; 2*offsetPos < until; offsetPos++){
      int start = startIndex(adjacentNodesOld, offsetPos);
      int length = blockLength(adjacentNodesOld, offsetPos);
      System.arraycopy(adjacentNodesOld.nodesWithEdgeProperties, start, nodesWithEdgePropertiesNew, off, length);
      res = res.setOffset(2 * offsetPos, off);
      res = res.setOffset(2 * offsetPos + 1, length);
      off += length;
    }
    int oldSize = adjacentNodesOld.nodesWithEdgeProperties.length;
    this.adjacentNodes = res;

    return (long) newSize + (((long) oldSize) << 32);
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
    long tmp = (id() ^ (id() >>> 33) ^ 0xc89f69faaa76b9b7L) * 0xa3ceded266465a8dL;
    return ((int) tmp) ^ ((int) (tmp >>> 32));
  }

  @Override
  public boolean equals(final Object obj) {
    return (this == obj) || ( (obj instanceof NodeDb) && id() == ((Node) obj).id() );
  }
}
