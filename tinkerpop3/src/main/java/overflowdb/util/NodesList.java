package overflowdb.util;

import gnu.trove.map.TLongIntMap;
import gnu.trove.map.hash.THashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.set.hash.THashSet;
import overflowdb.NodeRef;
import overflowdb.OdbNode;

import java.util.Set;

// TODO: this collection is only growing - intermittently trim it, e.g. if many elements have been deleted, after a GC run - note: must reeindex nodeIndexByNodeId
public class NodesList {
  private NodeRef[] nodes;
  private int size = 0;
  // TODO make use when adding elements
//  private static final int MAX_ARRAY_SIZE = 2147483639;
  private static final int DEFAULT_CAPACITY = 10000;

  //index into `nodes` array by node id
  private final TLongIntMap nodeIndexByNodeId;
  private final THashMap<String, Set<NodeRef>> nodesByLabel;

  /** list of available slots in `nodes` array. slots become available after nodes have been removed */
//  private final int[] emptySlots;

  public NodesList() {
    this(DEFAULT_CAPACITY);
  }

  public NodesList(int initialCapacity) {
    nodes = new NodeRef[initialCapacity];
    nodeIndexByNodeId = new TLongIntHashMap(10000);
    nodesByLabel = new THashMap<>(10);
  }

  /** store NodeRef in internal collections */
  public synchronized void add(NodeRef node) {
    int index = size++;
    nodes[index] = node;
    nodeIndexByNodeId.put(node.id, index);
    nodesByLabel(node.label()).add(node);
  }

  public NodeRef nodeById(long id) {
    if (nodeIndexByNodeId.containsKey(id)) {
      return nodes[nodeIndexByNodeId.get(id)];
    } else {
      return null;
    }
  }

  public Set<NodeRef> nodesByLabel(String label) {
    if (!nodesByLabel.containsKey(label))
      nodesByLabel.put(label, new THashSet<>(10));

    return nodesByLabel.get(label);
  }

  protected void removeNode(OdbNode node) {
//    int index = nodeIndexByNodeId.remove(node.id2());
//    nodes.remove(index);
//
//    indexManager.removeElement(node.ref);
//    nodesByLabel.get(node.label()).remove(node.ref);
//    storage.removeNode(node.id2());
  }

  public int size() {
    return size;
  }
}
