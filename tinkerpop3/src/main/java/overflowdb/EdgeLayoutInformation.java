package overflowdb;

import java.util.Set;

/**
 * Edges only exist as a virtual concept in OverflowDbNode.
 * This is used to instantiate NodeLayoutInformation.
 */
public class EdgeLayoutInformation {
  public final String label;
  public final Set<String> propertyKeys;

  public EdgeLayoutInformation(String label, Set<String> propertyKeys) {
    this.label = label;
    this.propertyKeys = propertyKeys;
  }
}
