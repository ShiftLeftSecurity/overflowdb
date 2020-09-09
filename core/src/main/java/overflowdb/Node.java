package overflowdb;

import java.util.Iterator;
import java.util.Map;

public interface Node extends Element {

  /**
   * Add an outgoing edge to the node with provided label and edge properties as key/value pairs.
   * These key/values must be provided in an even number where the odd numbered arguments are {@link String}
   * property keys and the even numbered arguments are the related property values.
   */
  Edge addEdge(String label, Node inNode, Object... keyValues);

  /**
   * Add an outgoing edge to the node with provided label and edge properties as key/value pairs.
   */
  Edge addEdge(String label, Node inNode, Map<String, Object> keyValues);

  /**
   * Add an outgoing edge to the node with provided label and edge properties as key/value pairs.
   * These key/values must be provided in an even number where the odd numbered arguments are {@link String}
   * property keys and the even numbered arguments are the related property values.
   * Just like {{{addEdge2}}, but doesn't instantiate and return a dummy edge
   */
  void addEdgeSilent(String label, Node inNode, Object... keyValues);

  /**
   * Add an outgoing edge to the node with provided label and edge properties as key/value pairs.
   * Just like {{{addEdge2}}, but doesn't instantiate and return a dummy edge
   */
  void addEdgeSilent(String label, Node inNode, Map<String, Object> keyValues);

  long id();

  /* adjacent OUT nodes (all labels) */
  Iterator<Node> out();

  /* adjacent OUT nodes for given labels */
  Iterator<Node> out(String... edgeLabels);

  /* adjacent IN nodes (all labels) */
  Iterator<Node> in();

  /* adjacent IN nodes for given labels */
  Iterator<Node> in(String... edgeLabels);

  /* adjacent OUT/IN nodes (all labels) */
  Iterator<Node> both();

  /* adjacent OUT/IN nodes for given labels */
  Iterator<Node> both(String... edgeLabels);

  /* adjacent OUT edges (all labels) */
  Iterator<Edge> outE();

  /* adjacent OUT edges for given labels */
  Iterator<Edge> outE(String... edgeLabels);

  /* adjacent IN edges (all labels) */
  Iterator<Edge> inE();

  /* adjacent IN edges for given labels */
  Iterator<Edge> inE(String... edgeLabels);

  /* adjacent OUT/IN edges (all labels) */
  Iterator<Edge> bothE();

  /* adjacent OUT/IN edges for given labels */
  Iterator<Edge> bothE(String... edgeLabels);
}
