package overflowdb.util;

import gnu.trove.map.TLongIntMap;
import gnu.trove.map.hash.THashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.set.hash.THashSet;
import overflowdb.NodeRef;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.Set;

public class NodesList {
  private NodeRef[] nodes;
  private int size = 0;

  //index into `nodes` array by node id
  private final TLongIntMap nodeIndexByNodeId;
  private final THashMap<String, Set<NodeRef>> nodesByLabel;

  /** list of available slots in `nodes` array. slots become available after nodes have been removed */
  private final BitSet emptySlots;

  private static final int DEFAULT_CAPACITY = 10000;

  public NodesList() {
    this(DEFAULT_CAPACITY);
  }

  public NodesList(int initialCapacity) {
    nodes = new NodeRef[initialCapacity];
    emptySlots = new BitSet(initialCapacity);
    nodeIndexByNodeId = new TLongIntHashMap(initialCapacity);
    nodesByLabel = new THashMap<>(10);
  }

  public Iterator<NodeRef> iterator() {
    return new Iterator<NodeRef>() {
      // TODO copy array to avoid concurrent modification?
      private int currIdx = 0;
      @Override
      public boolean hasNext() {
        return currIdx < size;
      }

      @Override
      public NodeRef next() {
        return nodes[currIdx++];
      }
    };
  }

  /** store NodeRef in internal collections */
  public synchronized void add(NodeRef node) {
    verifyUniqueId(node);
    int index = tryClaimEmptySlot();
    if (index == -1) {
      // no empty spot available - append to nodes array instead
      index = size;
      ensureCapacity(size + 1);
    }

    nodes[index] = node;
    nodeIndexByNodeId.put(node.id, index);
    nodesByLabel(node.label()).add(node);
    size++;
  }

  private void verifyUniqueId(NodeRef node) {
    if (nodeIndexByNodeId.containsKey(node.id)) {
      NodeRef existingNode = nodeById(node.id);
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

  protected void remove(NodeRef node) {
    int index = nodeIndexByNodeId.remove(node.id2());
    nodes[index] = null;
    emptySlots.set(index);
    nodesByLabel.get(node.label()).remove(node);
    size--;
  }

  public int size() {
    return size;
  }

  private void ensureCapacity(int minCapacity) {
    if (nodes.length < minCapacity) grow(minCapacity);
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
}
