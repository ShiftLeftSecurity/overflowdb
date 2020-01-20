package io.shiftleft.overflowdb;

import java.util.List;
import java.util.Set;

public final class OdbIndexManager {

  protected final NodePropertiesIndex nodeIndex;

  public OdbIndexManager(OdbGraph graph) {
    this.nodeIndex = new NodePropertiesIndex(graph);
  }

  /**
   * Create an index for specified node property.
   * Whenever an element has the specified key mutated, the index is updated.
   * When the index is created, all existing elements are indexed to ensure that they are captured by the index.
   */
  public final void createNodePropertyIndex(final String key) {
    nodeIndex.createKeyIndex(key);
  }

  /**
   * Drop the index for specified node property.
   */
  public final void dropNodePropertyIndex(final String key) {
    nodeIndex.dropKeyIndex(key);
  }

  /**
   * Return all the keys currently being indexed for nodes.
   */
  public final Set<String> getIndexedNodeProperties() {
    return nodeIndex.getIndexedKeys();
  }

  public final List<NodeRef> lookup(final String key, final Object value) {
    return nodeIndex.get(key, value);
  }

  public void removeElement(NodeRef ref) {
    nodeIndex.removeElement(ref);
  }
}
