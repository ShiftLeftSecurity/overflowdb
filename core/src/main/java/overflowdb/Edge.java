package overflowdb;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import overflowdb.util.IteratorUtils;

public abstract class Edge implements Element {
  private final Graph graph;
  private final String label;
  private final NodeRef outNode;
  private final NodeRef inNode;

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

  public Edge(Graph graph,
              String label,
              NodeRef outNode,
              NodeRef inVertex,
              Set<String> specificKeys) {
    this.graph = graph;
    this.label = label;
    this.outNode = outNode;
    this.inNode = inVertex;

    this.specificKeys = specificKeys;
    graph.referenceManager.applyBackpressureMaybe();
  }

  public NodeRef outNode() {
    return outNode;
  }

  public NodeRef inNode() {
    return inNode;
  }

  public Iterator<NodeRef> bothNodes() {
    return IteratorUtils.from(outNode, inNode);
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
  public String label() {
    return label;
  }

  @Override
  public Graph graph() {
    return graph;
  }

  @Override
  public void setProperty(String key, Object value) {
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
    inNode.get().setEdgeProperty(Direction.IN, label, key, value, inBlockOffset);
    outNode.get().setEdgeProperty(Direction.OUT, label, key, value, outBlockOffset);
  }

  @Override
  public Set<String> propertyKeys() {
    return specificKeys;
  }

  @Override
  public void removeProperty(String key) {
    inNode.get().removeEdgeProperty(Direction.IN, label, key, inBlockOffset);
    outNode.get().removeEdgeProperty(Direction.OUT, label, key, outBlockOffset);
  }

  @Override
  public void remove() {
    fixupBlockOffsets();
    outNode.get().removeEdge(Direction.OUT, label(), outBlockOffset);
    inNode.get().removeEdge(Direction.IN, label(), inBlockOffset);
  }

  @Override
  public Map<String, Object> propertyMap() {
    if (inBlockOffset != -1) {
      return inNode.get().edgePropertyMap(Direction.IN, this, getInBlockOffset());
    } else if (outBlockOffset != -1) {
      return outNode.get().edgePropertyMap(Direction.OUT, this, getOutBlockOffset());
    } else {
      throw new RuntimeException("Cannot get properties. In and out block offset uninitialized.");
    }
  }

  public Object property(String propertyKey) {
    if (inBlockOffset != -1) {
      return inNode.get().edgeProperty(Direction.IN, this, inBlockOffset, propertyKey);
    } else if (outBlockOffset != -1) {
      return outNode.get().edgeProperty(Direction.OUT, this, outBlockOffset, propertyKey);
    } else {
      throw new RuntimeException("Cannot get property. In and out block offset unitialized.");
    }
  }

  public boolean isRemoved() {
    return removed;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Edge)) {
      return false;
    }

    Edge otherEdge = (Edge) other;
    fixupBlockOffsetsIfNecessary(otherEdge);

    return this.inNode.id() == otherEdge.inNode.id() &&
        this.outNode.id() == otherEdge.outNode.id() &&
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
    return Objects.hash(inNode.id(), outNode.id(), label);
  }

  private void fixupBlockOffsetsIfNecessary(Edge otherEdge) {
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
        outNode.get().blockOffsetToOccurrence(Direction.OUT, label(), inNode, outBlockOffset);
    inBlockOffset = inNode.get().occurrenceToBlockOffset(Direction.IN, label(), outNode,
        edgeOccurenceForSameLabelEdgesBetweenSameNodePair);
  }

  private void initializeOutFromInOffset() {
    int edgeOccurenceForSameLabelEdgesBetweenSameNodePair =
        inNode.get().blockOffsetToOccurrence(Direction.IN, label(), outNode, inBlockOffset);
    outBlockOffset = outNode.get().occurrenceToBlockOffset(Direction.OUT, label(), inNode,
        edgeOccurenceForSameLabelEdgesBetweenSameNodePair);
  }

}
