package io.shiftleft.overflowdb;

import io.shiftleft.overflowdb.storage.OdbStorage;
import io.shiftleft.overflowdb.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

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

  public final int releaseCount = 100000; //TODO make configurable
  private AtomicInteger totalReleaseCount = new AtomicInteger(0);
  private final ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor(new NamedThreadFactory("overflowdb-reference-manager"));
  private int clearingProcessCount = 0;
  private final Object backPressureSyncObject = new Object();
  private final OdbStorage storage;

  private final List<NodeRef> clearableRefs = Collections.synchronizedList(new LinkedList<>());

  public ReferenceManager(OdbStorage storage) {
    this.storage = storage;
  }

  public void registerRef(NodeRef ref) {
    clearableRefs.add(ref);
  }

  /**
   * when we're running low on heap memory we'll serialize some elements to disk. to ensure we're not creating new ones
   * faster than old ones are serialized away, we're applying some backpressure in those situation
   */
  public void applyBackpressureMaybe() {
    synchronized (backPressureSyncObject) {
      while (clearingProcessCount > 0) {
        try {
          if (logger.isTraceEnabled()) logger.trace("wait until ref clearing completed");
          backPressureSyncObject.wait();
          if (logger.isTraceEnabled()) logger.trace("continue");
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
      if (logger.isInfoEnabled()) logger.info("scheduled to clear " + releaseCount + " references (asynchronously)");
      singleThreadExecutor.submit(() -> syncClearReferences(releaseCount));
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
      if (logger.isInfoEnabled()) logger.info("completed clearing of " + refsToClear.size() + " references");
      if (logger.isDebugEnabled()) logger.debug("current clearable queue size: " + clearableRefs.size());
      if (logger.isDebugEnabled()) logger.debug("references cleared in total: " + totalReleaseCount);
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
      clearReferences(refsToClear);
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

  private void clearReferences(final List<NodeRef> refsToClear) {
    serializeReferences(refsToClear.parallelStream().filter(NodeRef::isSet))
        .sequential()
        .forEach(serializedNode -> {
          serializedNode.ref.persist(serializedNode.data);
          serializedNode.ref.clear();
          totalReleaseCount.incrementAndGet();
        });
  }

  private static class SerializedNode {
    public final NodeRef ref;
    public final byte[] data;

    public SerializedNode(NodeRef ref, byte[] data) {
      this.ref = ref;
      this.data = data;
    }
  }

  private Stream<SerializedNode> serializeReferences(Stream<NodeRef> refs) {
    return refs
        .map(ReferenceManager::serializeReference)
        .filter(Objects::nonNull);
  }

  private static SerializedNode serializeReference(NodeRef ref) {
    final byte[] data = ref.serializeWhenDirty();
    if (data != null)
      return new SerializedNode(ref, data);
    else
      return null;
  }


  /**
   * writes all references to disk overflow, blocks until complete.
   * useful when saving the graph
   */
  public void clearAllReferences() {
    while (!clearableRefs.isEmpty()) {
      int clearableRefsSize = clearableRefs.size();
      logger.info("clearing all (" + clearableRefsSize + ") references - this may take some time");
      try {
        syncClearReferences(clearableRefsSize);
      } catch (Exception e) {
        throw new RuntimeException("error while clearing references to disk", e);
      }
    }
    logger.info("cleared all clearable references");
  }

  @Override
  public void close() {
    singleThreadExecutor.shutdown();
  }
}
