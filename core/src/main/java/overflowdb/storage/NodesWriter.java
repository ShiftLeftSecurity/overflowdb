package overflowdb.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import overflowdb.Node;
import overflowdb.NodeDb;
import overflowdb.NodeRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Persists collections of nodes in bulk to disk. Used either by ReferenceManager (if overflow to disk is enabled),
 * or alternatively when closing the graph (if storage to disk is enabled).
 */
public class NodesWriter {
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final boolean isLogInfoEnabled = logger.isInfoEnabled();
  public final int batchSize = 100000;
  private final NodeSerializer nodeSerializer;
  private final OdbStorage storage;

  public NodesWriter(NodeSerializer nodeSerializer, OdbStorage storage) {
    this.nodeSerializer = nodeSerializer;
    this.storage = storage;
  }

  /**
   * writes all references to storage, blocks until complete.
   *
   * @return number of persisted nodes
   */
  public int writeAndClearBatched(ArrayList<Node> nodes) {
    AtomicInteger count = new AtomicInteger(0);
    nodes.parallelStream().forEach(node -> {
      NodeDb nodeDb = null;
      NodeRef ref = null;
      if (node instanceof NodeDb) {
        nodeDb = (NodeDb) node;
        ref = nodeDb.ref;
      } else if (node instanceof NodeRef) {
        ref = (NodeRef) node;
        if (ref.isSet()) nodeDb = ref.get();
      }

      if (nodeDb.isDirty()) {
        try {
          byte[] bytes = nodeSerializer.serialize(nodeDb);
          storage.persist(ref.id(), bytes);
          ref.clear();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      int currCount = count.incrementAndGet();
      // log status every now and then (once in 2^16 times, i.e. once in 131072 times)
      if (isLogInfoEnabled && currCount >> 17 == 0) {
        float progressPercent = 100f * currCount / nodes.size();
        logger.info(String.format("progress of writing nodes to storage: %.2f%s", Float.min(100f, progressPercent), "%"));
      }
    });

    return count.get();
  }

}
