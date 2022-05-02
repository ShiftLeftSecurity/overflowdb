package overflowdb.util;

import overflowdb.Edge;
import overflowdb.Graph;
import overflowdb.Node;

import java.util.*;

public class DiffTool {

  /** compare two graphs element by element
   * identity of nodes is given by their id, i.e. if the graphs have the same nodes/edges/properties, but use different
   * node ids, a lot of differences will be reported and the results will be useless
   */
  public static List<String> compare(Graph graph1, Graph graph2) {
    final List<String> diff = new ArrayList<>();
    if (graph1.nodeCount() != graph2.nodeCount()) {
      diff.add(String.format("node count differs: graph1=%d, graph2=%d", graph1.nodeCount(), graph2.nodeCount()));
    }
    if (graph1.edgeCount() != graph2.edgeCount()) {
      diff.add(String.format("edge count differs: graph1=%d, graph2=%d", graph1.edgeCount(), graph2.edgeCount()));
    }

    SortedSet<Long> nodeIds = new TreeSet<>();
    graph1.nodes().forEachRemaining(node -> nodeIds.add(node.id()));
    graph2.nodes().forEachRemaining(node -> nodeIds.add(node.id()));

    nodeIds.forEach(nodeId -> {
      final Node node1 = graph1.node(nodeId);
      final Node node2 = graph2.node(nodeId);
      if (node1 == null) diff.add(String.format("node %s only exists in graph2", node2));
      else if (node2 == null) diff.add(String.format("node %s only exists in graph1", node1));
      else {
        if (!node1.label().equals(node2.label()))
          diff.add(String.format("different label for nodeId=%d; graph1=%s, graph2=%s ", nodeId, node1.label(), node2.label()));

        final String context = "nodeId=" + nodeId;
        compareProperties(node1.propertiesMap(), node2.propertiesMap(), diff, context);
        compareEdges(node1.outE(), node2.outE(), diff, context + ".outE");
      }
    });

    return diff;
  }

  private static void compareProperties(Map<String, Object> properties1, Map<String, Object> properties2, List<String> diff, String context) {
    SortedSet<String> propertyKeys = new TreeSet<>();
    propertyKeys.addAll(properties1.keySet());
    propertyKeys.addAll(properties2.keySet());

    propertyKeys.forEach(key -> {
      Object value1 = properties1.get(key);
      Object value2 = properties2.get(key);

      if (value1 == null) {
        diff.add(String.format("%s; property '%s' -> '%s' only exists in graph2", context, key, value2));
      } else if (value2 == null) {
        diff.add(String.format("%s; property '%s' -> '%s' only exists in graph1", context, key, value1));
      } else { // both values are not null
        if (value1.getClass().isArray() && value2.getClass().isArray()) { // both values are arrays
          if (!arraysEqual(value1, value2)) {
            diff.add(String.format("%s; array property '%s' has different values: graph1='%s', graph2='%s'", context, key, value1, value2));
          }
        } else if (!value1.equals(value2)) { // not both values are arrays
          diff.add(String.format("%s; property '%s' has different values: graph1='%s', graph2='%s'", context, key, value1, value2));
        }
      }
    });
  }


  /**
   * Compare given objects, assuming they are both arrays of the same type.
   * This is required because arrays don't support `.equals`, and is quite lengthy because java has one array type for each data type
   */
  public static boolean arraysEqual(Object value1, Object value2) {
    // need to check all array types unfortunately
    if (value1 instanceof Object[] && value2 instanceof Object[]) {
      return Arrays.deepEquals((Object[]) value1, (Object[]) value2);
    } else if (value1 instanceof boolean[] && value2 instanceof int[]) {
      return Arrays.equals((boolean[]) value1, (boolean[]) value2);
    } else if (value1 instanceof byte[] && value2 instanceof byte[]) {
      return Arrays.equals((byte[]) value1, (byte[]) value2);
    } else if (value1 instanceof char[] && value2 instanceof char[]) {
      return Arrays.equals((char[]) value1, (char[]) value2);
    } else if (value1 instanceof short[] && value2 instanceof short[]) {
      return Arrays.equals((short[]) value1, (short[]) value2);
    } else if (value1 instanceof int[] && value2 instanceof int[]) {
      return Arrays.equals((int[]) value1, (int[]) value2);
    } else if (value1 instanceof long[] && value2 instanceof long[]) {
      return Arrays.equals((long[]) value1, (long[]) value2);
    } else if (value1 instanceof float[] && value2 instanceof float[]) {
      return Arrays.equals((float[]) value1, (float[]) value2);
    } else if (value1 instanceof double[] && value2 instanceof double[]) {
      return Arrays.equals((double[]) value1, (double[]) value2);
    } else {
      throw new AssertionError(String.format(
        "unable to compare given objects (%s of type %s; %s of type %s)",
        value1, value1.getClass(), value2, value2.getClass()));
    }
  }

  private static void compareEdges(Iterator<Edge> edges1, Iterator<Edge> edges2, List<String> diff, String context) {
    List<Edge> edges1Sorted = sort(edges1);
    List<Edge> edges2Sorted = sort(edges2);

    if (edges1Sorted.size() != edges2Sorted.size()) {
      diff.add(String.format("%s; different number of edges: graph1=%d, graph2=%d", context, edges1Sorted.size(), edges2Sorted.size()));
    } else {
      Iterator<Edge> edges1SortedIter = edges1Sorted.iterator();
      Iterator<Edge> edges2SortedIter = edges2Sorted.iterator();
      while (edges1SortedIter.hasNext()) {
        Edge edge1 = edges1SortedIter.next();
        Edge edge2 = edges2SortedIter.next();

        if (!edge1.label().equals(edge2.label()))
          diff.add(String.format("%s; different label for sorted edges; graph1=%s, graph2=%s ", context, edge1.label(), edge2.label()));
        else
          compareProperties(edge1.propertiesMap(), edge2.propertiesMap(), diff, String.format("%s; edge label = %s", context, edge1.label()));
      }
    }
  }

  private static List<Edge> sort(Iterator<Edge> edges) {
    List<Edge> edgesSorted = new LinkedList();
    edges.forEachRemaining(edgesSorted::add);
    edgesSorted.sort(Comparator.comparing(edge ->
      String.format("%s %d", edge.label(), edge.propertiesMap().size())
    ));
    return edgesSorted;
  }


}
