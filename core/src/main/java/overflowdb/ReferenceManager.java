package overflowdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import overflowdb.storage.NodesWriter;
import overflowdb.storage.OdbStorage;
import overflowdb.util.NamedThreadFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * can clear references to disk and apply backpressure when creating new nodes, both to avoid an OutOfMemoryError
 *
 * can save all references to disk to persist the graph on shutdown
 * n.b. we could also persist the graph without a ReferenceManager, by serializing all nodes to disk. But if that
 * instance has been started from a storage location, the ReferenceManager ensures that we don't re-serialize all
 * unchanged nodes.
 */
public class ReferenceManager implements AutoCloseable, HeapUsageMonitor.HeapNotificationListener {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  public final int releaseCount = 100000;
  private AtomicInteger totalReleaseCount = new AtomicInteger(0);
  private final ExecutorService executorService;
  private final boolean shutdownExecutorOnClose;
  private int clearingProcessCount = 0;
  private final Object backPressureSyncObject = new Object();
  private final OdbStorage storage;
  private final NodesWriter nodesWriter;
  private final List<NodeRef> clearableRefs = Collections.synchronizedList(new ArrayList<>());

  /**
   * Create a reference manager with the given storage and node writer set; also spawns and manages
   * a background thread for clearing references - if you'd like more control consider using
   * {@link #ReferenceManager(OdbStorage, NodesWriter, ExecutorService)} instead.
   */
  public ReferenceManager(OdbStorage storage, NodesWriter nodesWriter) {
    this.storage = storage;
    this.nodesWriter = nodesWriter;
    this.executorService = Executors.newSingleThreadExecutor(new NamedThreadFactory("overflowdb-reference-manager"));
    this.shutdownExecutorOnClose = true;
  }

  /**
   * Create a reference manager with the given storage and node writer set; the given executor will be used to spawn
   * a background thread for clearing references.  Note that the executor will not be shut down once {@link #close()}
   * is called, it's the callers responsibility to manage it.
   */
  public ReferenceManager(OdbStorage storage, NodesWriter nodesWriter, ExecutorService executorService) {
    this.storage = storage;
    this.nodesWriter = nodesWriter;
    this.executorService = executorService;
    this.shutdownExecutorOnClose = false;
  }

  /* Register NodeRef, so it can be cleared on low memory */
  public void registerRef(NodeRef ref) {
    clearableRefs.add(ref);
  }

  /**
   * When we're running low on heap memory we'll serialize some elements to disk. To ensure we're not creating new ones
   * faster than old ones are serialized away, we're applying some backpressure to those newly created ones.
   */
  public void applyBackpressureMaybe() {
    synchronized (backPressureSyncObject) {
      while (clearingProcessCount > 0) {
        try {
          logger.trace("wait until ref clearing completed");
          backPressureSyncObject.wait();
          logger.trace("continue");
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  @Override
  public void notifyHeapAboveThreshold() {
    if (clearingProcessCount > 0) {
      logger.debug("cleaning in progress, will only queue up more references to clear after that's completed");
    } else if (clearableRefs.isEmpty()) {
      logger.info("no refs to clear at the moment, i.e. the heap is used by other components");
    } else {
      int releaseCount = Integer.min(this.releaseCount, clearableRefs.size());
      logger.info("scheduled to clear " + releaseCount + " references (asynchronously)");
      executorService.submit(() -> syncClearReferences(releaseCount));
    }
  }

  /**
   * run clearing of references asynchronously to not block the gc notification thread
   * using executor with one thread and capacity=1, drop `clearingInProgress` flag
   */
  private void syncClearReferences(final int releaseCount) {
    final List<NodeRef> refsToClear = collectRefsToClear(releaseCount);
    if (!refsToClear.isEmpty()) {
      safelyClearReferences(refsToClear);
      logger.info("completed clearing of " + refsToClear.size() + " references");
      logger.debug("remaining clearable references: " + clearableRefs.size());
      logger.trace("references cleared in total: " + totalReleaseCount);
    }
  }

  private List<NodeRef> collectRefsToClear(int releaseCount) {
    final List<NodeRef> refsToClear = new ArrayList<>(releaseCount);

    while (releaseCount > 0) {
      if (clearableRefs.isEmpty()) {
        break;
      }
      final NodeRef ref = clearableRefs.remove(0);
      if (ref != null) {
        refsToClear.add(ref);
      }
      releaseCount--;
    }

    return refsToClear;
  }

  /**
   * clear references, ensuring no exception is raised
   */
  private void safelyClearReferences(final List<NodeRef> refsToClear) {
    try {
      synchronized (backPressureSyncObject) {
        clearingProcessCount += 1;
      }
      nodesWriter.writeAndClearBatched(refsToClear.spliterator(), refsToClear.size());
      storage.flush();
    } catch (Exception e) {
      logger.error("error while trying to clear references", e);
    } finally {
      synchronized (backPressureSyncObject) {
        clearingProcessCount -= 1;
        if (clearingProcessCount == 0) {
          backPressureSyncObject.notifyAll();
        }
      }
    }
  }

  /**
   * writes all references to disk overflow, blocks until complete.
   * useful when saving the graph
   */
  public void clearAllReferences() {
    nodesWriter.writeAndClearBatched(clearableRefs.spliterator(), clearableRefs.size());
    logger.debug("cleared all clearable references");
  }

  @Override
  public void close() {
    if (shutdownExecutorOnClose) {
      executorService.shutdown();
    }
  }
}
