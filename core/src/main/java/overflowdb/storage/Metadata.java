package overflowdb.storage;

import overflowdb.Direction;

import java.util.Map;
import java.util.Set;

public class Metadata {
  // TODO later
  public int schemaVersion() {
    return -1;
  };

  // TODO later
  public int schemaHash() {
    return -1;
  };

  public Set<EdgeOffset> edgeOffsets(int nodeTypeId) {
    // TODO impl
    return null;
  }

  public static class EdgeOffset {
    public final int forNodeTypeId;
    public final String edgeType;
    public final Direction direction;

    public EdgeOffset(int forNodeTypeId, String edgeType, Direction direction) {
      this.forNodeTypeId = forNodeTypeId;
      this.edgeType = edgeType;
      this.direction = direction;
    }
  }

}

