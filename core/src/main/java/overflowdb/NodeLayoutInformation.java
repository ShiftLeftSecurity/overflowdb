package overflowdb;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Contains all static node-specific information for serialization / deserialization.
 * <p>
 * Please make sure to instantiate only one instance per node type to not waste memory.
 */
public class NodeLayoutInformation {
  /** unique id for this node's label
   *  This is mostly an optimization for storage - we could as well serialize labels as string, but numbers are more efficient.
   *  Since we know our schema at compile time, we can assign unique ids for each label.
   *  */
  public final int labelId;

  private final Set<String> propertyKeys;
  private final String[] allowedOutEdgeLabels;
  private final String[] allowedInEdgeLabels;

  /* position for given OUT edge label in edgeOffsets */
  private final Map<String, Integer> outEdgeToOffsetPosition;

  /* position for given IN edge label in edgeOffsets */
  private final Map<String, Integer> inEdgeToOffsetPosition;

  /* possible edge property keys, grouped by edge label.
   * n.b. property keys are of type `HashSet` (rather than just `Set`) to ensure `.size` has constant time */
  private final Map<String, HashSet<String>> edgePropertyKeysByLabel;

  /* maps offsetPos -> number of edge properties*/
  private final int[] edgePropertyCountByOffsetPosition;

  /* position in stride (entry within `adjacentNodesWithEdgeProperties`) for a given edge label and edge property key
   * 1-based, because index `0` is the adjacent node ref */
  private final Map<LabelAndKey, Integer> edgeLabelAndKeyToStrideIndex;

  public NodeLayoutInformation(int labelId,
                               Set<String> propertyKeys,
                               List<EdgeLayoutInformation> outEdgeLayouts,
                               List<EdgeLayoutInformation> inEdgeLayouts) {
    this.labelId = labelId;
    this.propertyKeys = propertyKeys;

    Set<EdgeLayoutInformation> allEdgeLayouts = new HashSet<>();
    allEdgeLayouts.addAll(outEdgeLayouts);
    allEdgeLayouts.addAll(inEdgeLayouts);

    edgePropertyKeysByLabel = createEdgePropertyKeysByLabel(allEdgeLayouts);
    edgeLabelAndKeyToStrideIndex = createEdgeLabelAndKeyToStrideIndex(allEdgeLayouts);

    /* create unique offsets for each edge type and direction
     * ordered by input order (first out, then in, same order as in outEdgeLayouts)
     * this ensures that (1) we get the same offsets between restarts and (2) the offsets are easily apparent from the
     * constructor call. Downstream may rely on the offsets, beware before changing!
     * schema changes will change layout*/

    allowedOutEdgeLabels = new String[outEdgeLayouts.size()];
    allowedInEdgeLabels = new String[inEdgeLayouts.size()];
    int offsetPosition = 0;
    edgePropertyCountByOffsetPosition = new int[outEdgeLayouts.size() + inEdgeLayouts.size()];
    int i = 0;
    outEdgeToOffsetPosition = new HashMap<>(outEdgeLayouts.size());
    for (EdgeLayoutInformation edgeLayout : outEdgeLayouts) {
      edgePropertyCountByOffsetPosition[offsetPosition] = edgePropertyKeysByLabel.get(edgeLayout.label).size();
      outEdgeToOffsetPosition.put(edgeLayout.label, offsetPosition++);
      allowedOutEdgeLabels[i++] = edgeLayout.label;
    }
    i = 0;
    inEdgeToOffsetPosition = new HashMap<>(inEdgeLayouts.size());
    for (EdgeLayoutInformation edgeLayout : inEdgeLayouts) {
      edgePropertyCountByOffsetPosition[offsetPosition] = edgePropertyKeysByLabel.get(edgeLayout.label).size();
      inEdgeToOffsetPosition.put(edgeLayout.label, offsetPosition++);
      allowedInEdgeLabels[i++] = edgeLayout.label;
    }
  }

  private Map<String, HashSet<String>> createEdgePropertyKeysByLabel(Set<EdgeLayoutInformation> allEdgeLayouts) {
    Map<String, HashSet<String>> edgePropertyKeysByLabel = new HashMap<>(allEdgeLayouts.size());
    for (EdgeLayoutInformation edgeLayout : allEdgeLayouts) {
      edgePropertyKeysByLabel.put(edgeLayout.label, new HashSet<>(edgeLayout.propertyKeys));
    }
    return edgePropertyKeysByLabel;
  }

  private Map<LabelAndKey, Integer> createEdgeLabelAndKeyToStrideIndex(Set<EdgeLayoutInformation> allEdgeLayouts) {
    Map<LabelAndKey, Integer> edgeLabelAndKeyToStrideIndex = new HashMap<>(allEdgeLayouts.size());
    for (EdgeLayoutInformation edgeLayout : allEdgeLayouts) {
      /* 1-based, because index `0` is the adjacent node ref */
      int strideIndex = 1;

      /* sort property keys to ensure we get the same offsets between restarts
       * n.b. this doesn't support schema changes */
      for (String propertyKey : sorted(edgeLayout.propertyKeys)) {
        edgeLabelAndKeyToStrideIndex.put(new LabelAndKey(edgeLayout.label, propertyKey), strideIndex++);
      }
    }
    return edgeLabelAndKeyToStrideIndex;
  }

  private Iterable<String> sorted(Set<String> propertyKeys) {
    SortedSet<String> sortedSet = new TreeSet<>(String::compareTo);
    sortedSet.addAll(propertyKeys);
    return sortedSet;
  }

  private SortedSet<EdgeLayoutInformation> sortByLabel(List<EdgeLayoutInformation> outEdgeLayouts) {
    SortedSet<EdgeLayoutInformation> sorted = new TreeSet<>(Comparator.comparing(a -> a.label));
    sorted.addAll(outEdgeLayouts);
    return sorted;
  }

  public Set<String> propertyKeys() {
    return propertyKeys;
  }

  public String[] allowedOutEdgeLabels() {
    return allowedOutEdgeLabels;
  }

  public String[] allowedInEdgeLabels() {
    return allowedInEdgeLabels;
  }

  public Set<String> edgePropertyKeys(String edgeLabel) {
    return edgePropertyKeysByLabel.get(edgeLabel);
  }

  /* The number of different IN|OUT edge relations. E.g. a node has AST edges in and out, then we would have 2.
   * If in addition it has incoming ref edges it would have 3. */
  public int numberOfDifferentAdjacentTypes() {
    return outEdgeToOffsetPosition.size() + inEdgeToOffsetPosition.size();
  }

  /* position for given OUT edge label in OverflowDbNode.edgeOffsets */
  public Integer outEdgeToOffsetPosition(String edgeLabel) {
    return outEdgeToOffsetPosition.get(edgeLabel);
  }

  /* position for given IN edge label in OverflowDbNode.edgeOffsets */
  public Integer inEdgeToOffsetPosition(String edgeLabel) {
    return inEdgeToOffsetPosition.get(edgeLabel);
  }

  /**
   * @return The offset relative to the adjacent node in the adjacentNodesWithEdgeProperties array starting from 1.
   * Return -1 if property key does not exist for given edgeLabel.
   */
  public int getEdgePropertyOffsetRelativeToAdjacentNodeRef(String edgeLabel, String propertyKey) {
    return edgeLabelAndKeyToStrideIndex.getOrDefault(new LabelAndKey(edgeLabel, propertyKey), -1);
  }

  /* gets edge property count by offsetPos*/
  public final int getEdgePropertyCountByOffsetPos(int offsetPos) {
    return edgePropertyCountByOffsetPosition[offsetPos];
  }

  class LabelAndKey {
    final String label;
    final String propertyKey;

    LabelAndKey(String label, String propertyKey) {
      this.label = label;
      this.propertyKey = propertyKey;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      LabelAndKey that = (LabelAndKey) o;
      return label.equals(that.label) &&
          propertyKey.equals(that.propertyKey);
    }

    @Override
    public int hashCode() {
      return Objects.hash(label, propertyKey);
    }
  }


}

