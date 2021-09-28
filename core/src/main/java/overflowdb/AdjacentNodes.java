package overflowdb;

/**
 * AdjacentNodes is the storage container for adjacent nodes, used in NodeDb.
 *
 * This class is really package private; it is only formally public to simplify internal organization of overflowdb.
 */
public class AdjacentNodes {

  /**
   * holds refs to all adjacent nodes (a.k.a. dummy edges) and the edge properties
   */
  public final Object[] nodesWithEdgeProperties;

  /** store the start offset and length into the above `adjacentNodesWithEdgeProperties` array in an interleaved manner,
   * i.e. each adjacent edge type has two entries in this array.
   * The type is one of byte[], short[] or int[]*/
  final Object offsets;

  /**
   * create an empty AdjacentNodes container for a node with @param numberOfDifferentAdjacentTypes
   */
  AdjacentNodes(int numberOfDifferentAdjacentTypes) {
    nodesWithEdgeProperties = new Object[0];
    offsets = new byte[numberOfDifferentAdjacentTypes * 2];
  }


  AdjacentNodes(Object[] nodesWithEdgeProperties, Object offsets) {
    this.nodesWithEdgeProperties = nodesWithEdgeProperties;
    this.offsets = offsets;
  }
  /**
   * getOffset(2 * kindOffset) gets the offset into nodesWithEdgeProperties of the desired kindOffset (get the kindOffset from layout info)
   * getOffset(2 * kindOffset + 1) gets the length of the relevant slice in nodesWithEdgeProperties of the desired kindOffset (get the kindOffset from layout info)
   */
  public int getOffset(int pos){
    if(offsets instanceof byte[]){
      return ((byte[]) offsets)[pos];
    } else if(offsets instanceof short[]){
      return ((short[]) offsets)[pos];
    } else if (offsets instanceof int[]){
      return ((int[]) offsets)[pos];
    } else throw new RuntimeException("corrupt state: offsets of type " + offsets.getClass().getName());
  }

  /** Attempts to update AdjacentNodes in-place and return this; otherwise, create a new AdjacentNodes and return that.
   * */
  AdjacentNodes setOffset(int pos, int val){
    if(offsets instanceof byte[]) {
      if(val == (byte) val){
        ((byte[]) offsets)[pos] = (byte) val;
        return this;
      } else if (val == (short) val){
        byte[] oldOffsets = (byte[]) offsets;
        short[] newOffsets = new short[oldOffsets.length];
        for(int i = 0; i < oldOffsets.length; i++){
          newOffsets[i] = oldOffsets[i];
        }
        newOffsets[pos] = (short) val;
        return new AdjacentNodes(nodesWithEdgeProperties, newOffsets);
      } else {
        byte[] oldOffsets = (byte[]) offsets;
        int[] newOffsets = new int[oldOffsets.length];
        for(int i = 0; i < oldOffsets.length; i++){
          newOffsets[i] = oldOffsets[i];
        }
        newOffsets[pos] = val;
        return new AdjacentNodes(nodesWithEdgeProperties, newOffsets);
      }
    } else if (offsets instanceof short[]){
      if(val == (short) val){
        ((short[]) offsets)[pos] = (short) val;
        return this;
      } else {
        short[] oldOffsets = (short[]) offsets;
        int[] newOffsets = new int[oldOffsets.length];
        for(int i = 0; i < oldOffsets.length; i++){
          newOffsets[i] = oldOffsets[i];
        }
        newOffsets[pos] = val;
        return new AdjacentNodes(nodesWithEdgeProperties, newOffsets);
      }
    } else if (offsets instanceof int[]){
      ((int[]) offsets)[pos] = val;
      return this;
    } else {
      throw new RuntimeException("corrupt state: offsets of type " + offsets.getClass().getName());
    }
  }

  public int offsetLengths(){
    if(offsets instanceof int[]) return ((int[]) offsets).length;
    else if (offsets instanceof short[]) return ((short[]) offsets).length;
    else if (offsets instanceof byte[]) return ((byte[]) offsets).length;
    else throw new RuntimeException();
  }

}
