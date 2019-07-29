package io.shiftleft.overflowdb.structure;

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
 * Contains all static node-specific information. This could in theory be part of OverflowDbNode, but to save memory
 * we need to minimize the amount of fields per node instance. While there may be millions of node instances, there's
 * only one NodeLayoutInformation instance per node type, which holds this static information.
 * <p>
 * Please make sure to instantiate only one instance per node type to not waste memory.
 */
public class NodeLayoutInformation {
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

  /* position in stride (entry within `adjacentVerticesWithProperties`) for a given edge label and edge property key
   * 1-based, because index `0` is the adjacent node ref */
  private final Map<LabelAndKey, Integer> edgeLabelAndKeyToStrideIndex;

  public NodeLayoutInformation(Set<String> propertyKeys,
                               List<EdgeLayoutInformation> outEdgeLayouts,
                               List<EdgeLayoutInformation> inEdgeLayouts) {
    this.propertyKeys = propertyKeys;

    Set<EdgeLayoutInformation> allEdgeLayouts = new HashSet<>();
    allEdgeLayouts.addAll(outEdgeLayouts);
    allEdgeLayouts.addAll(inEdgeLayouts);

    edgePropertyKeysByLabel = createEdgePropertyKeysByLabel(allEdgeLayouts);
    edgeLabelAndKeyToStrideIndex = createEdgeLabelAndKeyToStrideIndex(allEdgeLayouts);

    /* create unique offsets for each edge type and direction
     * sort them by edge label to ensure we get the same offsets between restarts
     * n.b. this doesn't support schema changes */
    int offsetPosition = 0;
    outEdgeToOffsetPosition = new HashMap<>(outEdgeLayouts.size());
    for (EdgeLayoutInformation edgeLayout : sortByLabel(outEdgeLayouts)) {
      outEdgeToOffsetPosition.put(edgeLayout.label, offsetPosition++);
    }
    inEdgeToOffsetPosition = new HashMap<>(inEdgeLayouts.size());
    for (EdgeLayoutInformation edgeLayout : sortByLabel(inEdgeLayouts)) {
      inEdgeToOffsetPosition.put(edgeLayout.label, offsetPosition++);
    }

    /* only so we don't need to calculate it every time */
    allowedOutEdgeLabels = outEdgeToOffsetPosition.keySet().toArray(new String[0]);
    allowedInEdgeLabels = inEdgeToOffsetPosition.keySet().toArray(new String[0]);
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

  /* The number fo different IN|OUT edge relations. E.g. a node has AST edges in and out, then we would have 2.
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
   * @return The offset relative to the adjacent vertex element in the
   * adjacentVerticesWithProperties array starting from 1. Return -1 if
   * key does not exist for given edgeLabel.
   */
  public int getOffsetRelativeToAdjacentVertexRef(String edgeLabel, String key) {
    return edgeLabelAndKeyToStrideIndex.getOrDefault(new LabelAndKey(edgeLabel, key), -1);
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

