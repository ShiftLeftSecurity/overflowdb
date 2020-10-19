package overflowdb.storage;

import overflowdb.Direction;

public class EdgeOffset {
  public final int nodeTypeId;
  public final Direction direction;
  public final String edgeType;
  public final int offset;

  public EdgeOffset(int nodeTypeId, Direction direction, String edgeType, int offset) {
    this.nodeTypeId = nodeTypeId;
    this.direction = direction;
    this.edgeType = edgeType;
    this.offset = offset;
  }
}
