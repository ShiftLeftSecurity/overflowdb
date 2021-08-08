package overflowdb;

import overflowdb.util.PackedIntArray;

public class AdjacentNodes {

  /**
   * holds refs to all adjacent nodes (a.k.a. dummy edges) and the edge properties
   */
  public final Object[] nodesWithEdgeProperties;

  /* store the start offset and length into the above `adjacentNodesWithEdgeProperties` array in an interleaved manner,
   * i.e. each adjacent edge type has two entries in this array. */
  public final PackedIntArray edgeOffsets;

  /**
   * create an empty AdjacentNodes container for a node with @param numberOfDifferentAdjacentTypes
   */
  public AdjacentNodes(int numberOfDifferentAdjacentTypes) {
    nodesWithEdgeProperties = new Object[0];
    edgeOffsets = PackedIntArray.create(numberOfDifferentAdjacentTypes * 2);
  }

  public AdjacentNodes(Object[] nodesWithEdgeProperties, PackedIntArray edgeOffsets) {
    this.nodesWithEdgeProperties = nodesWithEdgeProperties;
    this.edgeOffsets = edgeOffsets;
  }

}
