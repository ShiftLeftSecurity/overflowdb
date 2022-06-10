package overflowdb;

import overflowdb.util.PropertyHelper;

import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

public abstract class Node extends Element implements NodeOrDetachedNode {

  /**
   * Add an outgoing edge to the node with provided label and edge properties as key/value pairs.
   * These key/values must be provided in an even number where the odd numbered arguments are {@link String}
   * property keys and the even numbered arguments are the related property values.
   */
  @Deprecated public final Edge addEdge(String label, Node inNode, Object... keyValues){return addEdgeImpl(label, inNode, keyValues);}
  protected abstract Edge addEdgeImpl(String label, Node inNode, Object... keyValues);
  final Edge addEdgeInternal(String label, Node inNode, Object... keyValues){return addEdgeImpl(label, inNode, keyValues);}


  /**
   * Add an outgoing edge to the node with provided label and edge properties as key/value pairs.
   */
  @Deprecated public final Edge addEdge(String label, Node inNode, Map<String, Object> keyValues){return addEdgeImpl(label, inNode, keyValues);}
  protected abstract Edge addEdgeImpl(String label, Node inNode, Map<String, Object> keyValues);
  final Edge addEdgeInternal(String label, Node inNode, Map<String, Object> keyValues){return addEdgeImpl(label, inNode, keyValues);}


  /**
   * Add an outgoing edge to the node with provided label and edge properties as key/value pairs.
   * These key/values must be provided in an even number where the odd numbered arguments are {@link String}
   * property keys and the even numbered arguments are the related property values.
   * Just like {{{addEdge2}}, but doesn't instantiate and return a dummy edge
   */
  @Deprecated public final void addEdgeSilent(String label, Node inNode, Object... keyValues){addEdgeSilentImpl(label, inNode, keyValues);}
  protected abstract void addEdgeSilentImpl(String label, Node inNode, Object... keyValues);
  final void addEdgeSilentInternal(String label, Node inNode, Object... keyValues){addEdgeSilentImpl(label, inNode, keyValues);}


  /**
   * Add an outgoing edge to the node with provided label and edge properties as key/value pairs.
   * Just like {{{addEdge2}}, but doesn't instantiate and return a dummy edge
   */
  @Deprecated public final void addEdgeSilent(String label, Node inNode, Map<String, Object> keyValues){addEdgeSilentImpl(label, inNode, keyValues);}
  protected abstract void addEdgeSilentImpl(String label, Node inNode, Map<String, Object> keyValues);
  final void addEdgeSilentInternal(String label, Node inNode, Map<String, Object> keyValues){addEdgeSilentImpl(label, inNode, keyValues);}


  public abstract long id();

  /* adjacent OUT nodes (all labels) */
  public abstract Iterator<Node> out();

  /* adjacent OUT nodes for given labels */
  public abstract Iterator<Node> out(String... edgeLabels);

  /* adjacent IN nodes (all labels) */
  public abstract Iterator<Node> in();

  /* adjacent IN nodes for given labels */
  public abstract Iterator<Node> in(String... edgeLabels);

  /* adjacent OUT/IN nodes (all labels) */
  public abstract Iterator<Node> both();

  /* adjacent OUT/IN nodes for given labels */
  public abstract Iterator<Node> both(String... edgeLabels);

  /* adjacent OUT edges (all labels) */
  public abstract Iterator<Edge> outE();

  /* adjacent OUT edges for given labels */
  public abstract Iterator<Edge> outE(String... edgeLabels);

  /* adjacent IN edges (all labels) */
  public abstract Iterator<Edge> inE();

  /* adjacent IN edges for given labels */
  public abstract Iterator<Edge> inE(String... edgeLabels);

  /* adjacent OUT/IN edges (all labels) */
  public abstract Iterator<Edge> bothE();

  /* adjacent OUT/IN edges for given labels */
  public abstract Iterator<Edge> bothE(String... edgeLabels);

  /*Allows fast initialization from detached node data*/
  protected void _initializeFromDetached(DetachedNodeData data, Function<DetachedNodeData, Node> mapper){
    throw new RuntimeException("Detached initialization is not supported by node type " + label() + " of class " + getClass().getName() );
  }
  /*Allows fast initialization from detached node data; available as static instead of instance method, because we need to keep the REPL clean*/
  static void initializeFromDetached(Node node, DetachedNodeData data, Function<DetachedNodeData, Node> refMapper){
    if (data instanceof DetachedNodeGeneric) {
      PropertyHelper.attachProperties(node, ((DetachedNodeGeneric) data).propertiesAsKeyValues);
    } else node._initializeFromDetached(data, refMapper);
  }
}
