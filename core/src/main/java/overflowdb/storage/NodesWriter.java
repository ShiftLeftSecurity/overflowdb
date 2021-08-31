package overflowdb.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import overflowdb.Node;
import overflowdb.NodeDb;
import overflowdb.NodeRef;

import java.io.IOException;
import java.util.Spliterator;
import java.util.stream.StreamSupport;

/**
 * Persists collections of nodes in bulk to disk. Used either by ReferenceManager (if overflow to disk is enabled),
 * or alternatively when closing the graph (if storage to disk is enabled).
 */
public class NodesWriter {
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final boolean isLogInfoEnabled = logger.isInfoEnabled();
  private static final int LogAfterXSerializedNodes = 100_000;
  private final NodeSerializer nodeSerializer;
  private final OdbStorage storage;

  public NodesWriter(NodeSerializer nodeSerializer, OdbStorage storage) {
    this.nodeSerializer = nodeSerializer;
    this.storage = storage;
  }

  /**
   * writes all references to storage, blocks until complete.
   */
  public void writeAndClearBatched(Spliterator<? extends Node> nodes, int estimatedTotalCount) {
    CountEstimates countEstimates = new CountEstimates();

    StreamSupport.stream(nodes, false).forEach(node -> {
      NodeDb nodeDb = null;
      NodeRef ref = null;
      if (node instanceof NodeDb) {
        nodeDb = (NodeDb) node;
        ref = nodeDb.ref;
      } else if (node instanceof NodeRef) {
        ref = (NodeRef) node;
        if (ref.isSet()) nodeDb = ref.get();
      }

      if (nodeDb != null && nodeDb.isDirty()) {
        try {
          byte[] bytes = nodeSerializer.serialize(nodeDb);
          storage.persist(ref.id(), bytes);
          NodeRef.clear(ref);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      if (isLogInfoEnabled) {
        countEstimates.countEstimate++;
        countEstimates.nodesToSerializeUntilLogging--;
        if (countEstimates.nodesToSerializeUntilLogging == 0) {
          float progressPercent = 100f * countEstimates.countEstimate / estimatedTotalCount;
          countEstimates.nodesToSerializeUntilLogging = LogAfterXSerializedNodes;
          logger.info(String.format("progress of writing nodes to storage: %.2f%s", Float.min(100f, progressPercent), "%"));
        }
      }
    });
  }

  /**
   * Counts aren't atomic or synchronized, but worst case we'll miss or duplicate a log.info message,
   * which is not a big problem :tm:
   * They're only wrapped in a separate object to trick the compiler into thinking that these are 'final or effectively final'
   */
  private static class CountEstimates {
    int countEstimate = 0;
    int nodesToSerializeUntilLogging = LogAfterXSerializedNodes;
  }

}
