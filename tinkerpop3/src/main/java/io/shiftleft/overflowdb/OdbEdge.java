package io.shiftleft.overflowdb;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

public abstract class OdbEdge implements Edge, OdbElement {
  private final OdbGraph graph;
  private final String label;
  private final NodeRef outVertex;
  private final NodeRef inVertex;

  /* When storing the inVertex in the outVertex' adjacent node array, there may be multiple edges
   * with the same (direction, label), i.e. they are stored in the same block. To be able to
   * identify this edge, we store it's offset into that block */
  private int outBlockOffset = UNINITIALIZED_BLOCK_OFFSET;

  /**
   * When storing the outVertex in the inVertex' adjacent node array, there may be multiple edges
   * with the same (direction, label), i.e. they are stored in the same block. To be able to
   * identify this edge, we store it's offset into that block
   */
  private int inBlockOffset = UNINITIALIZED_BLOCK_OFFSET;

  private final Set<String> specificKeys;
  private boolean removed = false;

  private static final int UNINITIALIZED_BLOCK_OFFSET = -1;

  public OdbEdge(OdbGraph graph,
                 String label,
                 NodeRef outVertex,
                 NodeRef inVertex,
                 Set<String> specificKeys) {
    this.graph = graph;
    this.label = label;
    this.outVertex = outVertex;
    this.inVertex = inVertex;

    this.specificKeys = specificKeys;
    graph.referenceManager.applyBackpressureMaybe();
  }

  public int getOutBlockOffset() {
    return outBlockOffset;
  }

  public void setOutBlockOffset(int offset) {
    outBlockOffset = offset;
  }

  public int getInBlockOffset() {
    return inBlockOffset;
  }

  public void setInBlockOffset(int offset) {
    inBlockOffset = offset;
  }

  @Override
  public Iterator<Vertex> vertices(Direction direction) {
    switch (direction) {
      case OUT:
        return IteratorUtils.of(outVertex);
      case IN:
        return IteratorUtils.of(inVertex);
      default:
        return IteratorUtils.of(outVertex, inVertex);
    }
  }

  @Override
  public Object id() {
    return this;
  }

  @Override
  public String label() {
    return label;
  }

  @Override
  public Graph graph() {
    return graph;
  }

  @Override
  public OdbGraph graph2() {
    return graph;
  }

  @Override
  public <V> Property<V> property(String key, V value) {
    // TODO check if it's an allowed property key
    if (inBlockOffset != UNINITIALIZED_BLOCK_OFFSET) {
      if (outBlockOffset == UNINITIALIZED_BLOCK_OFFSET) {
        initializeOutFromInOffset();
      }
    } else if (outBlockOffset != UNINITIALIZED_BLOCK_OFFSET) {
      if (inBlockOffset == UNINITIALIZED_BLOCK_OFFSET) {
        initializeInFromOutOffset();
      }
    } else {
      throw new RuntimeException("Cannot set property. In and out block offset uninitialized.");
    }
    inVertex.get().setEdgeProperty(Direction.IN, label, key, value, inBlockOffset);
    outVertex.get().setEdgeProperty(Direction.OUT, label, key, value, outBlockOffset);
    return new OdbProperty<>(key, value, this);
  }

  @Override
  public <P> void setProperty(String key, P value) {
    this.property(key, value);
  }

  @Override
  public Set<String> keys() {
    return specificKeys;
  }

  @Override
  public void remove() {
    fixupBlockOffsets();
    outVertex.get().removeEdge(Direction.OUT, label(), outBlockOffset);
    inVertex.get().removeEdge(Direction.IN, label(), inBlockOffset);
  }

  @Override
  public <V> Iterator<Property<V>> properties(String... propertyKeys) {
    if (inBlockOffset != -1) {
      return inVertex.get().getEdgeProperties(Direction.IN, this, getInBlockOffset(), propertyKeys);
    } else if (outBlockOffset != -1) {
      return outVertex.get().getEdgeProperties(Direction.OUT, this, getOutBlockOffset(), propertyKeys);
    } else {
      throw new RuntimeException("Cannot get properties. In and out block offset uninitialized.");
    }
  }

  @Override
  public <V> Property<V> property(String propertyKey) {
    if (inBlockOffset != -1) {
      return inVertex.get().getEdgeProperty(Direction.IN, this, inBlockOffset, propertyKey);
    } else if (outBlockOffset != -1) {
      return outVertex.get().getEdgeProperty(Direction.OUT, this, outBlockOffset, propertyKey);
    } else {
      throw new RuntimeException("Cannot get property. In and out block offset unitialized.");
    }
  }

  // TODO drop suffix `2` after tinkerpop interface is gone
  public <P> P property2(String propertyKey) {
    if (inBlockOffset != -1) {
      return inVertex.get().getEdgeProperty2(Direction.IN, this, inBlockOffset, propertyKey);
    } else if (outBlockOffset != -1) {
      return outVertex.get().getEdgeProperty2(Direction.OUT, this, outBlockOffset, propertyKey);
    } else {
      throw new RuntimeException("Cannot get property. In and out block offset unitialized.");
    }
  }

  public boolean isRemoved() {
    return removed;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof OdbEdge)) {
      return false;
    }

    OdbEdge otherEdge = (OdbEdge) other;
    fixupBlockOffsetsIfNecessary(otherEdge);

    return this.inVertex.id().equals(otherEdge.inVertex.id()) &&
        this.outVertex.id().equals(otherEdge.outVertex.id()) &&
        this.label.equals(otherEdge.label) &&
        (this.inBlockOffset == UNINITIALIZED_BLOCK_OFFSET ||
            otherEdge.inBlockOffset == UNINITIALIZED_BLOCK_OFFSET ||
            this.inBlockOffset == otherEdge.inBlockOffset) &&
        (this.outBlockOffset == UNINITIALIZED_BLOCK_OFFSET ||
            otherEdge.outBlockOffset == UNINITIALIZED_BLOCK_OFFSET ||
            this.outBlockOffset == otherEdge.outBlockOffset);
  }

  @Override
  public int hashCode() {
    // Despite the fact that the block offsets are used in the equals method,
    // we do not hash over the block offsets as those may change.
    // This results in hash collisions for edges with the same label between the
    // same nodes but since those are deemed very rare this is ok.
    return Objects.hash(inVertex.id(), outVertex.id(), label);
  }

  private void fixupBlockOffsetsIfNecessary(OdbEdge otherEdge) {
    if ((this.inBlockOffset == UNINITIALIZED_BLOCK_OFFSET ||
        otherEdge.inBlockOffset == UNINITIALIZED_BLOCK_OFFSET) &&
        (this.outBlockOffset == UNINITIALIZED_BLOCK_OFFSET ||
            otherEdge.outBlockOffset == UNINITIALIZED_BLOCK_OFFSET)) {
      fixupBlockOffsets();
    }
  }

  private void fixupBlockOffsets() {
    if (inBlockOffset == UNINITIALIZED_BLOCK_OFFSET) {
      initializeInFromOutOffset();
    }
    if (outBlockOffset == UNINITIALIZED_BLOCK_OFFSET) {
      initializeOutFromInOffset();
    }
  }

  private void initializeInFromOutOffset() {
    int edgeOccurenceForSameLabelEdgesBetweenSameNodePair =
        outVertex.get().blockOffsetToOccurrence(Direction.OUT, label(), inVertex, outBlockOffset);
    inBlockOffset = inVertex.get().occurrenceToBlockOffset(Direction.IN, label(), outVertex,
        edgeOccurenceForSameLabelEdgesBetweenSameNodePair);
  }

  private void initializeOutFromInOffset() {
    int edgeOccurenceForSameLabelEdgesBetweenSameNodePair =
        inVertex.get().blockOffsetToOccurrence(Direction.IN, label(), outVertex, inBlockOffset);
    outBlockOffset = outVertex.get().occurrenceToBlockOffset(Direction.OUT, label(), inVertex,
        edgeOccurenceForSameLabelEdgesBetweenSameNodePair);
  }

}
