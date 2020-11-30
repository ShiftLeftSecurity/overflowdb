package overflowdb.util;

import gnu.trove.map.TLongIntMap;
import gnu.trove.map.TMap;
import gnu.trove.map.hash.THashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.set.hash.THashSet;
import overflowdb.Node;
import overflowdb.NodeRef;
import overflowdb.NodeDb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

public class NodesList {
  private Node[] nodes;
  private int size = 0;

  //index into `nodes` array by node id
  private TLongIntMap nodeIndexByNodeId;
  private TMap<String, ArrayList<Node>> nodesByLabel;

  /** list of available slots in `nodes` array. slots become available after nodes have been removed */
  private final BitSet emptySlots;

  private static final int DEFAULT_CAPACITY = 10000;

  public NodesList() {
    this(DEFAULT_CAPACITY);
  }

  public NodesList(int initialCapacity) {
    nodes = new Node[initialCapacity];
    emptySlots = new BitSet(initialCapacity);
    nodeIndexByNodeId = new TLongIntHashMap(initialCapacity);
    nodesByLabel = new THashMap<>(10);
  }

  /** store Node in internal collections */
  public synchronized void add(Node node) {
    verifyUniqueId(node);
    int index = tryClaimEmptySlot();
    if (index == -1) {
      // no empty spot available - append to nodes array instead
      index = size;
      ensureCapacity(size + 1);
    }

    nodes[index] = node;
    nodeIndexByNodeId.put(node.id(), index);
    if(nodesByLabel != null) {
      nodesByLabel(node.label()).add(node);
    }
    size++;
  }

  private void verifyUniqueId(Node node) {
    if (nodeIndexByNodeId.containsKey(node.id())) {
      Node existingNode = nodeById(node.id());
      throw new AssertionError("different Node with same id already exists in this NodesList: " + existingNode);
    }
  }

  /** @return -1 if no available empty slots, otherwise the successfully claimed slot */
  private int tryClaimEmptySlot() {
    final int nextEmptySlot = emptySlots.nextSetBit(0);
    if (nextEmptySlot != -1) {
      emptySlots.clear(nextEmptySlot);
    }
    return nextEmptySlot;
  }

  public boolean contains(long id) {
    return nodeIndexByNodeId.containsKey(id);
  }

  public Node nodeById(long id) {
    if (nodeIndexByNodeId.containsKey(id)) {
      return nodes[nodeIndexByNodeId.get(id)];
    } else {
      return null;
    }
  }

  public void remove(Node node) {
    int index = nodeIndexByNodeId.remove(node.id());
    nodes[index] = null;
    emptySlots.set(index);

    NodeRef ref = node instanceof NodeDb
        ? ((NodeDb) node).ref
        : (NodeRef) node;
    this.nodesByLabel = null;

    size--;
    compactMaybe();
  }

  public int size() {
    return size;
  }


  synchronized private void refreshNodesByLabel(){
    TMap<String, ArrayList<Node>> tmp = new THashMap<>();
    for(Node node: nodes){
      if(node != null){
        ArrayList<Node> nodelist = tmp.get(node.label());
        if(nodelist == null){
          nodelist = new ArrayList<>();
          tmp.put(node.label(), nodelist);
        }
        nodelist.add(node);
      }
    }
    this.nodesByLabel = tmp;
  }

  public ArrayList<Node> nodesByLabel(String label) {
    if(nodesByLabel == null) refreshNodesByLabel();
    ArrayList<Node> nodelist = nodesByLabel.get(label);
    if(nodelist == null){
      nodelist = new ArrayList<>();
      nodesByLabel.put(label, nodelist);
    }
    return nodelist;
  }

  public Set<String> nodeLabels() {
    Set<String> ret = new HashSet<>(nodesByLabel.size());
    nodesByLabel.entrySet().forEach(entry -> {
      if (!entry.getValue().isEmpty()) {
        ret.add(entry.getKey());
      }
    });
    return ret;
  }

  public Iterator<Node> iterator() {
    return new NodesIterator(Arrays.copyOf(nodes, nodes.length));
  }

  private void ensureCapacity(int minCapacity) {
    if (nodes.length < minCapacity) grow(minCapacity);
  }

  /** compact if there are many empty slots, and they make up >= 30% of the node array */
  private void compactMaybe() {
    final int emptyCount = emptySlots.cardinality();
    if (emptyCount > 10000 &&
        emptyCount * 100 / nodes.length >= 30) {
      compact();
    }
  }

  /** trims down internal collections to just about the necessary size, in order to allow the remainder to be
   * garbage collected */
  public void compact() {
    final ArrayList<Node> newNodes = new ArrayList<>(size);
    Iterator<Node> iter = iterator();
    while (iter.hasNext()) {
      newNodes.add(iter.next());
    }
    nodes = newNodes.toArray(new Node[size]);

    //reindex helper collections
    emptySlots.clear();
    nodeIndexByNodeId = new TLongIntHashMap(this.nodes.length);

    int idx = 0;
    while (idx < this.nodes.length) {
      Node node = this.nodes[idx];
      nodeIndexByNodeId.put(node.id(), idx);
      idx++;
    }
  }

  /** The maximum size of array to allocate.
   * Some VMs reserve some header words in an array.
   * Attempts to allocate larger arrays may result in
   * OutOfMemoryError: Requested array size exceeds VM limit
   * @see java.util.ArrayList (copied from there)
   */
  private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

  /** Increases the capacity to ensure that it can hold at least the
   * number of elements specified by the minimum capacity argument.
   * @see java.util.ArrayList (copied from there) */
  private void grow(int minCapacity) {
    // overflow-conscious code
    int oldCapacity = nodes.length;
    int newCapacity = oldCapacity + (oldCapacity >> 1);
    if (newCapacity - minCapacity < 0)
      newCapacity = minCapacity;
    if (newCapacity - MAX_ARRAY_SIZE > 0)
      newCapacity = hugeCapacity(minCapacity);
    // minCapacity is usually close to size, so this is a win:
    nodes = Arrays.copyOf(nodes, newCapacity);
  }

 /** @see java.util.ArrayList (copied from there) */
  private static int hugeCapacity(int minCapacity) {
    if (minCapacity < 0) // overflow
      throw new OutOfMemoryError();
    return (minCapacity > MAX_ARRAY_SIZE) ?
        Integer.MAX_VALUE :
        MAX_ARRAY_SIZE;
  }

  /** just for unit test */
  protected int _elementDataSize() {
    return nodes.length;
  }

  /** cardinality of nodes for given label */
  public int cardinality(String label) {
    if (nodesByLabel.containsKey(label))
      return nodesByLabel.get(label).size();
    else
      return 0;
  }

  public static class NodesIterator implements Iterator<Node> {
    final Node[] nodes;
    int idx = 0;
    Node nextPeeked = null;

    public NodesIterator(Node[] nodes) {
      this.nodes = nodes;
    }

    @Override
    public boolean hasNext() {
      while (nextPeeked == null && idx < nodes.length) {
        nextPeeked = nodes[idx++];
      }
      return nextPeeked != null;
    }

    @Override
    public Node next() {
      if (!hasNext()) throw new NoSuchElementException("next on empty iterator");
      else {
        Node ret = nextPeeked;
        nextPeeked = null;
        return ret;
      }
    }
  }

}
