package overflowdb.tinkerpop;

import overflowdb.Direction;

public class Converters {
  public static Direction fromTinker(org.apache.tinkerpop.gremlin.structure.Direction tinkerDirection) {
    switch (tinkerDirection) {
      case OUT: return Direction.OUT;
      case IN: return Direction.IN;
      case BOTH: return Direction.BOTH;
      default: throw new AssertionError("unknown tinkerpop Direction: " + tinkerDirection);
    }
  }
}
