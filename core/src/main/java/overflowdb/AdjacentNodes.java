package overflowdb;

public class AdjacentNodes {

  /**
   * holds refs to all adjacent nodes (a.k.a. dummy edges) and the edge properties
   */
  public final Object[] nodesWithEdgeProperties;

  /* store the start offset and length into the above `adjacentNodesWithEdgeProperties` array in an interleaved manner,
   * i.e. each adjacent edge type has two entries in this array.
   * The type is one of byte[], short[] or int[]*/
  public final Object offsets;

  /**
   * create an empty AdjacentNodes container for a node with @param numberOfDifferentAdjacentTypes
   */
  public AdjacentNodes(int numberOfDifferentAdjacentTypes) {
    nodesWithEdgeProperties = new Object[0];
    offsets = new byte[numberOfDifferentAdjacentTypes * 2];
  }


  public AdjacentNodes(Object[] nodesWithEdgeProperties, Object offsets) {
    this.nodesWithEdgeProperties = nodesWithEdgeProperties;
    if(offsets == null) throw new RuntimeException("NPE");
    if(offsets instanceof byte[] | offsets instanceof short[] | offsets instanceof int[])
      this.offsets = offsets;
    else throw new RuntimeException();
  }

  public int getOffset(int pos){
    if(offsets instanceof byte[]){
      return ((byte[]) offsets)[pos];
    } else if(offsets instanceof short[]){
      return ((short[]) offsets)[pos];
    } else if (offsets instanceof int[]){
      return ((int[]) offsets)[pos];
    } else throw new RuntimeException(offsets.getClass().getName());
  }

  //returns null if the insertion was in-place; returns a new AdjacentNodes if we need to update
  public AdjacentNodes setOffset(int pos, int val){
    if(offsets instanceof byte[]) {
      if(val == (byte) val){
        ((byte[]) offsets)[pos] = (byte) val;
        return null;
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
        return null;
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
      return null;
    } else {
      throw new RuntimeException();
    }
  }

  public int getIntOffsetLen(){
    if(offsets instanceof int[]) return ((int[]) offsets).length;
    else if (offsets instanceof short[]) return ((short[]) offsets).length;
    else if (offsets instanceof byte[]) return ((byte[]) offsets).length;
    else throw new RuntimeException(offsets.getClass().getName());
  }

}
