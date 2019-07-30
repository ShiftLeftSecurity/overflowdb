package io.shiftleft.overflowdb.structure;

import io.shiftleft.overflowdb.storage.iterator.MultiIterator2;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

/**
 * Node/Vertex that stores adjacent Nodes directly, rather than via edges.
 * Motivation: in many graph use cases, edges don't hold any properties and thus accounts for more memory and
 * traversal time than necessary
 */
public abstract class OdbNode implements Vertex {

  public final NodeRef ref;

  /**
   * holds refs to all adjacent nodes (a.k.a. dummy edges) and the edge properties
   */
  private Object[] adjacentVerticesWithProperties = new Object[0];

  /* store the start offset and length into the above `adjacentVerticesWithProperties` array in an interleaved manner,
   * i.e. each outgoing edge type has two entries in this array. */
  private int[] edgeOffsets;

  protected OdbNode(NodeRef ref) {
    this.ref = ref;

    ref.setNode(this);
    ref.graph.referenceManager.applyBackpressureMaybe();

    edgeOffsets = new int[layoutInformation().numberOfDifferentAdjacentTypes() * 2];
  }

  protected abstract NodeLayoutInformation layoutInformation();

  protected abstract <V> Iterator<VertexProperty<V>> specificProperties(String key);

  public Object[] getAdjacentVerticesWithProperties() {
    return adjacentVerticesWithProperties;
  }

  public void setAdjacentVerticesWithProperties(Object[] adjacentVerticesWithProperties) {
    this.adjacentVerticesWithProperties = adjacentVerticesWithProperties;
  }

  public int[] getEdgeOffsets() {
    return edgeOffsets;
  }

  public void setEdgeOffsets(int[] edgeOffsets) {
    this.edgeOffsets = edgeOffsets;
  }

  public abstract Map<String, Object> valueMap();

  @Override
  public Graph graph() {
    return ref.graph;
  }

  @Override
  public Object id() {
    return ref.id;
  }

  @Override
  public Set<String> keys() {
    return layoutInformation().propertyKeys();
  }

  @Override
  public <V> VertexProperty<V> property(String key) {
    return specificProperty(key);
  }

  /* You can override this default implementation in concrete specialised instances for performance
   * if you like, since technically the Iterator isn't necessary.
   * This default implementation works fine though. */
  protected <V> VertexProperty<V> specificProperty(String key) {
    Iterator<VertexProperty<V>> iter = specificProperties(key);
    if (iter.hasNext()) {
      return iter.next();
    } else {
      return VertexProperty.empty();
    }
  }

  @Override
  public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
    if (propertyKeys.length == 0) { // return all properties
      return (Iterator) layoutInformation().propertyKeys().stream().flatMap(key ->
          StreamSupport.stream(Spliterators.spliteratorUnknownSize(
              specificProperties(key), Spliterator.ORDERED), false)
      ).iterator();
    } else if (propertyKeys.length == 1) { // treating as special case for performance
      return specificProperties(propertyKeys[0]);
    } else {
      return (Iterator) Arrays.stream(propertyKeys).flatMap(key ->
          StreamSupport.stream(Spliterators.spliteratorUnknownSize(
              specificProperties(key), Spliterator.ORDERED), false)
      ).iterator();
    }
  }

  @Override
  public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
    ElementHelper.legalPropertyKeyValueArray(keyValues);
    ElementHelper.validateProperty(key, value);
    synchronized (this) {
//            this.modifiedSinceLastSerialization = true;
      final VertexProperty<V> vp = updateSpecificProperty(cardinality, key, value);
      Index.autoUpdateIndex(this, key, value, null);
      return vp;
    }
  }

  protected abstract <V> VertexProperty<V> updateSpecificProperty(
      VertexProperty.Cardinality cardinality, String key, V value);

  protected abstract void removeSpecificProperty(String key);

  @Override
  public void remove() {
    OdbGraph graph = ref.graph;
    final List<Edge> edges = new ArrayList<>();
    this.edges(Direction.BOTH).forEachRemaining(edges::add);
    for (Edge edge : edges) {
      if (!((OdbEdge) edge).isRemoved()) {
        edge.remove();
      }
    }
    Index.removeElementIndex(this);
    graph.nodes.remove(ref.id);
    graph.getElementsByLabel(graph.nodesByLabel, label()).remove(this);

    graph.ondiskOverflow.removeVertex(ref.id);
//        this.modifiedSinceLastSerialization = true;
  }

//    public void setModifiedSinceLastSerialization(boolean modifiedSinceLastSerialization) {
//        this.modifiedSinceLastSerialization = modifiedSinceLastSerialization;
//    }

  public <V> Iterator<Property<V>> getEdgeProperties(Direction direction,
                                                     OdbEdge edge,
                                                     int blockOffset,
                                                     String... keys) {
    List<Property<V>> result = new ArrayList<>();

    if (keys.length != 0) {
      for (String key : keys) {
        result.add(getEdgeProperty(direction, edge, blockOffset, key));
      }
    } else {
      for (String propertyKey : layoutInformation().edgePropertyKeys(edge.label())) {
        result.add(getEdgeProperty(direction, edge, blockOffset, propertyKey));
      }
    }

    return result.iterator();
  }

  public <V> Property<V> getEdgeProperty(Direction direction,
                                         OdbEdge edge,
                                         int blockOffset,
                                         String key) {
    int propertyPosition = getEdgePropertyIndex(direction, edge.label(), key, blockOffset);
    if (propertyPosition == -1) {
      return EmptyProperty.instance();
    }
    V value = (V) adjacentVerticesWithProperties[propertyPosition];
    if (value == null) {
      return EmptyProperty.instance();
    }
    return new OdbProperty<>(key, value, edge);
  }

  public <V> void setEdgeProperty(Direction direction,
                                  String edgeLabel,
                                  String key,
                                  V value,
                                  int blockOffset) {
    int propertyPosition = getEdgePropertyIndex(direction, edgeLabel, key, blockOffset);
    if (propertyPosition == -1) {
      throw new RuntimeException("Edge " + edgeLabel + " does not support property " + key + ".");
    }
    adjacentVerticesWithProperties[propertyPosition] = value;
  }

  private int calcAdjacentVertexIndex(Direction direction,
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
    int adjacentVertexIndex = calcAdjacentVertexIndex(direction, label, blockOffset);
    if (adjacentVertexIndex == -1) {
      return -1;
    }

    int propertyOffset = layoutInformation().getOffsetRelativeToAdjacentVertexRef(label, key);
    if (propertyOffset == -1) {
      return -1;
    }

    return adjacentVertexIndex + propertyOffset;
  }

  @Override
  public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
    final NodeRef inNodeRef;
    if (inVertex instanceof NodeRef) inNodeRef = (NodeRef) inVertex;
    else inNodeRef = (NodeRef) ref.graph.vertex((Long) inVertex.id());
    OdbNode inVertexOdb = inNodeRef.get();
    NodeRef thisNodeRef = (NodeRef) ref.graph.vertex(ref.id);

    int outBlockOffset = storeAdjacentNode(Direction.OUT, label, inNodeRef, keyValues);
    int inBlockOffset = inVertexOdb.storeAdjacentNode(Direction.IN, label, thisNodeRef, keyValues);

    OdbEdge dummyEdge = instantiateDummyEdge(label, thisNodeRef, inNodeRef);
    dummyEdge.setOutBlockOffset(outBlockOffset);
    dummyEdge.setInBlockOffset(inBlockOffset);

    return dummyEdge;
  }

  @Override
  public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
    final MultiIterator2<Edge> multiIterator = new MultiIterator2<>();
    if (direction == Direction.IN || direction == Direction.BOTH) {
      for (String label : calcInLabels(edgeLabels)) {
        Iterator<Edge> edgeIterator = createDummyEdgeIterator(Direction.IN, label);
        multiIterator.addIterator(edgeIterator);
      }
    }
    if (direction == Direction.OUT || direction == Direction.BOTH) {
      for (String label : calcOutLabels(edgeLabels)) {
        Iterator<Edge> edgeIterator = createDummyEdgeIterator(Direction.OUT, label);
        multiIterator.addIterator(edgeIterator);
      }
    }

    return multiIterator;
  }

  @Override
  public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
    final MultiIterator2<Vertex> multiIterator = new MultiIterator2<>();
    if (direction == Direction.IN || direction == Direction.BOTH) {
      for (String label : calcInLabels(edgeLabels)) {
        multiIterator.addIterator(createAdjacentVertexIterator(Direction.IN, label));
      }
    }
    if (direction == Direction.OUT || direction == Direction.BOTH) {
      for (String label : calcOutLabels(edgeLabels)) {
        multiIterator.addIterator(createAdjacentVertexIterator(Direction.OUT, label));
      }
    }

    return multiIterator;
  }

  /**
   * If there are multiple edges between the same two nodes with the same label, we use the
   * `occurrence` to differentiate between those edges. Both nodes use the same occurrence
   * index for the same edge.
   *
   * @return the occurrence for a given edge, calculated by counting the number times the given
   * adjacent vertex occurred between the start of the edge-specific block and the blockOffset
   */
  public int blockOffsetToOccurrence(Direction direction,
                                     String label,
                                     NodeRef otherVertex,
                                     int blockOffset) {
    int offsetPos = getPositionInEdgeOffsets(direction, label);
    int start = startIndex(offsetPos);
    int strideSize = getStrideSize(label);

    int occurrenceCount = -1;
    for (int i = start; i <= start + blockOffset; i += strideSize) {
      if (((NodeRef) adjacentVerticesWithProperties[i]).id().equals(otherVertex.id())) {
        occurrenceCount++;
      }
    }
    return occurrenceCount;
  }

  /**
   * @param direction  OUT or IN
   * @param label      the edge label
   * @param occurrence if there are multiple edges between the same two nodes with the same label,
   *                   this is used to differentiate between those edges.
   *                   Both nodes use the same occurrence index for the same edge.
   * @return the index into `adjacentVerticesWithProperties`
   */
  public int occurrenceToBlockOffset(Direction direction,
                                     String label,
                                     NodeRef adjacentVertex,
                                     int occurrence) {
    int offsetPos = getPositionInEdgeOffsets(direction, label);
    int start = startIndex(offsetPos);
    int length = blockLength(offsetPos);
    int strideSize = getStrideSize(label);

    int currentOccurrence = 0;
    for (int i = start; i < start + length; i += strideSize) {
      if (((NodeRef) adjacentVerticesWithProperties[i]).id().equals(adjacentVertex.id())) {
        if (currentOccurrence == occurrence) {
          int adjacentVertexIndex = i - start;
          return adjacentVertexIndex;
        } else {
          currentOccurrence++;
        }
      }
    }
    throw new RuntimeException("Unable to find occurrence " + occurrence + " of "
        + label + " edge to vertex " + adjacentVertex.id());
  }

  /**
   * Removes an 'edge', i.e. in reality it removes the information about the adjacent node from
   * `adjacentVerticesWithProperties`. The corresponding elements will be set to `null`, i.e. we'll have holes.
   * Note: this decrements the `offset` of the following edges in the same block by one, but that's ok because the only
   * thing that matters is that the offset is identical for both connected nodes (assuming thread safety).
   *
   * @param blockOffset must have been initialized
   */
  protected void removeEdge(Direction direction, String label, int blockOffset) {
    int offsetPos = getPositionInEdgeOffsets(direction, label);
    int start = startIndex(offsetPos) + blockOffset;
    int strideSize = getStrideSize(label);

    for (int i = start; i < start + strideSize; i++) {
      adjacentVerticesWithProperties[i] = null;
    }
  }

  private Iterator<Edge> createDummyEdgeIterator(Direction direction,
                                                 String label) {
    int offsetPos = getPositionInEdgeOffsets(direction, label);
    if (offsetPos != -1) {
      int start = startIndex(offsetPos);
      int length = blockLength(offsetPos);
      int strideSize = getStrideSize(label);

      return new DummyEdgeIterator(adjacentVerticesWithProperties, start, start + length, strideSize,
          direction, label, (NodeRef) ref);
    } else {
      return Collections.emptyIterator();
    }
  }

  private Iterator<NodeRef> createAdjacentVertexIterator(Direction direction, String label) {
    int offsetPos = getPositionInEdgeOffsets(direction, label);
    if (offsetPos != -1) {
      int start = startIndex(offsetPos);
      int length = blockLength(offsetPos);
      int strideSize = getStrideSize(label);

      return new ArrayOffsetIterator<>(adjacentVerticesWithProperties, start, start + length, strideSize);
    } else {
      return Collections.emptyIterator();
    }
  }

  private int storeAdjacentNode(Direction direction,
                                String edgeLabel,
                                NodeRef nodeRef,
                                Object... edgeKeyValues) {
    int blockOffset = storeAdjacentNode(direction, edgeLabel, nodeRef);

    /* set edge properties */
    for (int i = 0; i < edgeKeyValues.length; i = i + 2) {
      if (!edgeKeyValues[i].equals(T.id) && !edgeKeyValues[i].equals(T.label)) {
        String key = (String) edgeKeyValues[i];
        Object value = edgeKeyValues[i + 1];
        setEdgeProperty(direction, edgeLabel, key, value, blockOffset);
      }
    }

    return blockOffset;
  }

  private int storeAdjacentNode(Direction direction, String edgeLabel, NodeRef nodeRef) {
    int offsetPos = getPositionInEdgeOffsets(direction, edgeLabel);
    if (offsetPos == -1) {
      throw new RuntimeException("Edge of type " + edgeLabel + " with direction " + direction +
          " not supported by class " + getClass().getSimpleName());
    }
    int start = startIndex(offsetPos);
    int length = blockLength(offsetPos);
    int strideSize = getStrideSize(edgeLabel);

    int insertAt = start + length;
    if (adjacentVerticesWithProperties.length <= insertAt || adjacentVerticesWithProperties[insertAt] != null) {
      // space already occupied - grow adjacentVerticesWithProperties array, leaving some room for more elements
      adjacentVerticesWithProperties = growAdjacentVerticesWithProperties(offsetPos, strideSize, insertAt, length);
    }

    adjacentVerticesWithProperties[insertAt] = nodeRef;
    // update edgeOffset length to include the newly inserted element
    edgeOffsets[2 * offsetPos + 1] = length + strideSize;

    int blockOffset = length;
    return blockOffset;
  }

  private int startIndex(int offsetPosition) {
    return edgeOffsets[2 * offsetPosition];
  }

  /**
   * @return number of elements reserved in `adjacentVerticesWithProperties` for a given edge label
   * includes space for the node ref and all properties
   */
  private int getStrideSize(String edgeLabel) {
    int sizeForNodeRef = 1;
    Set<String> allowedPropertyKeys = layoutInformation().edgePropertyKeys(edgeLabel);
    return sizeForNodeRef + allowedPropertyKeys.size();
  }

  /**
   * @return The position in edgeOffsets array. -1 if the edge label is not supported
   */
  private int getPositionInEdgeOffsets(Direction direction, String label) {
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
   * Returns the length of an edge type block in the adjacentVerticesWithProperties array.
   * Length means number of index positions.
   */
  private int blockLength(int offsetPosition) {
    return edgeOffsets[2 * offsetPosition + 1];
  }

  private String[] calcInLabels(String... edgeLabels) {
    if (edgeLabels.length != 0) {
      return edgeLabels;
    } else {
      return layoutInformation().allowedInEdgeLabels();
    }
  }

  private String[] calcOutLabels(String... edgeLabels) {
    if (edgeLabels.length != 0) {
      return edgeLabels;
    } else {
      return layoutInformation().allowedOutEdgeLabels();
    }
  }

  /**
   * grow the adjacentVerticesWithProperties array
   * <p>
   * preallocates more space than immediately necessary, so we don't need to grow the array every time
   * (tradeoff between performance and memory).
   * grows with the square root of the double of the current capacity.
   */
  private Object[] growAdjacentVerticesWithProperties(int offsetPos,
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
    int newSize = adjacentVerticesWithProperties.length + additionalEntriesCount;
    Object[] newArray = new Object[newSize];
    System.arraycopy(adjacentVerticesWithProperties, 0, newArray, 0, insertAt);
    System.arraycopy(adjacentVerticesWithProperties, insertAt, newArray, insertAt + additionalEntriesCount, adjacentVerticesWithProperties.length - insertAt);

    // Increment all following start offsets by `additionalEntriesCount`.
    for (int i = offsetPos + 1; 2 * i < edgeOffsets.length; i++) {
      edgeOffsets[2 * i] = edgeOffsets[2 * i] + additionalEntriesCount;
    }
    return newArray;
  }

  /**
   * to follow the tinkerpop api, instantiate and return a dummy edge, which doesn't really exist in the graph
   */
  protected OdbEdge instantiateDummyEdge(String label,
                                         NodeRef outVertex,
                                         NodeRef inVertex) {
    final OdbElementFactory.ForEdge edgeFactory = ref.graph.edgeFactoryByLabel.get(label);
    if (edgeFactory == null)
      throw new IllegalArgumentException("specializedEdgeFactory for label=" + label + " not found - please register on startup!");
    return edgeFactory.createEdge(ref.graph, outVertex, inVertex);
  }
}
