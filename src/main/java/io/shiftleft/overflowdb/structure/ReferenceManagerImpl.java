/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.shiftleft.overflowdb.structure;

import com.sun.management.GarbageCollectionNotificationInfo;

import javax.management.ListenerNotFoundException;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.MemoryUsage;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** watches GC activity, and when we're low on available heap space,
 * it sets some references in the `[Vertex|Edge]Refs` to `null`, to avoid OOM
 * */
public class ReferenceManagerImpl implements ReferenceManager {
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private Map<NotificationEmitter, NotificationListener> gcNotificationListeners = new HashMap<>(2);
  private final float heapUsageThreshold; // range 0.0 - 1.0
  public final int releaseCount = 100000; //TODO make configurable
  private AtomicInteger totalReleaseCount = new AtomicInteger(0);
  private final Integer cpuCount = Runtime.getRuntime().availableProcessors();
  private final ExecutorService executorService = Executors.newFixedThreadPool(cpuCount);
  private int clearingProcessCount = 0;
  private final Object backPressureSyncObject = new Object();

  private final List<ElementRef> clearableRefs = Collections.synchronizedList(new LinkedList<>());

  public ReferenceManagerImpl(int heapPercentageThreshold) {
    if (heapPercentageThreshold < 0 || heapPercentageThreshold > 100) {
      throw new IllegalArgumentException("heapPercentageThreshold must be between 0 and 100, but is " + heapPercentageThreshold);
    }
    heapUsageThreshold =  (float) heapPercentageThreshold / 100f;
    installGCMonitoring();
  }

  @Override
  public void registerRef(ElementRef ref) {
    clearableRefs.add(ref);
  }

  /** when we're running low on heap memory we'll serialize some elements to disk. to ensure we're not creating new ones
   * faster than old ones are serialized away, we're applying some backpressure in those situation */
  @Override
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

  protected void maybeClearReferences(final float heapUsage) {
    if (heapUsage > heapUsageThreshold) {
      if (clearingProcessCount > 0) {
        logger.debug("cleaning in progress, will only queue up more references to clear after that's completed");
      } else if (clearableRefs.isEmpty()) {
        logger.info("no refs to clear at the moment. heapUsage=" + heapUsage);
      } else {
        int releaseCount = Integer.min(this.releaseCount, clearableRefs.size());
        logger.info("heap usage (after GC) was " + heapUsage + " -> scheduled to clear " + releaseCount + " references (asynchronously)");
        asynchronouslyClearReferences(releaseCount);
      }
    }
  }

  /** run clearing of references asynchronously to not block the gc notification thread
   * using executor with one thread and capacity=1, drop `clearingInProgress` flag
   */
  protected List<Future> asynchronouslyClearReferences(final int releaseCount) {
    List<Future> futures = new ArrayList<>(cpuCount);
    // use Math.ceil to err on the larger side
    final int releaseCountPerThread = (int) Math.ceil(releaseCount / cpuCount.floatValue());
    for (int i = 0; i < cpuCount; i++) {
      // doing this concurrently is tricky and won't be much faster since PriorityBlockingQueue is `blocking` anyway
      final List<ElementRef> refsToClear = collectRefsToClear(releaseCountPerThread);
      if (!refsToClear.isEmpty()) {
        futures.add(executorService.submit(() -> {
          safelyClearReferences(refsToClear);
          logger.info("completed clearing of " + refsToClear.size() + " references");
          logger.debug("current clearable queue size: " + clearableRefs.size());
          logger.debug("references cleared in total: " + totalReleaseCount);
        }));
      }
    }
    return futures;
  }

  protected List<ElementRef> collectRefsToClear(int releaseCount) {
    final List<ElementRef> refsToClear = new ArrayList<>(releaseCount);

    while (releaseCount > 0) {
      if (clearableRefs.isEmpty()) {
        break;
      }
      final ElementRef ref = clearableRefs.remove(0);
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
  protected void safelyClearReferences(final List<ElementRef> refsToClear) {
    try {
      synchronized (backPressureSyncObject) {
        clearingProcessCount += 1;
      }
      clearReferences(refsToClear);
    } catch (Exception e) {
      logger.error("error while trying to clear " + refsToClear.size() + " references", e);
    } finally {
      synchronized (backPressureSyncObject) {
        clearingProcessCount -= 1;
        if (clearingProcessCount == 0) {
          backPressureSyncObject.notifyAll();
        }
      }
    }
  }

  protected void clearReferences(final List<ElementRef> refsToClear) throws IOException {
    logger.info("attempting to clear "+ refsToClear.size() + " references");
    final Iterator<ElementRef> refsIterator = refsToClear.iterator();
    while (refsIterator.hasNext()) {
      final ElementRef ref = refsIterator.next();
      if (ref.isSet()) {
        ref.clear();
        totalReleaseCount.incrementAndGet();
      }
    }
  }

  /** monitor GC, and should the heap grow above 80% usage, clear some strong references */
  protected void installGCMonitoring() {
    List<GarbageCollectorMXBean> gcbeans = java.lang.management.ManagementFactory.getGarbageCollectorMXBeans();
    for (GarbageCollectorMXBean gcbean : gcbeans) {
      NotificationListener listener = createNotificationListener();
      NotificationEmitter emitter = (NotificationEmitter) gcbean;
      emitter.addNotificationListener(listener, null, null);
      gcNotificationListeners.put(emitter, listener);
    }
    int heapUsageThresholdPercent = (int) Math.floor(heapUsageThreshold * 100f);
    logger.info("installed GC monitors. will clear references if heap (after GC) is larger than " + heapUsageThresholdPercent + "%");
  }

  private NotificationListener createNotificationListener() {
    Set<String> ignoredMemoryAreas = new HashSet<>(Arrays.asList("Code Cache", "Compressed Class Space", "Metaspace"));
    return (notification, handback) -> {
        if (notification.getType().equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION)) {
          GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo.from((CompositeData) notification.getUserData());

          //sum up used and max memory across relevant memory areas
          long totalMemUsed = 0;
          long totalMemMax = 0;
          for (Map.Entry<String, MemoryUsage> entry : info.getGcInfo().getMemoryUsageAfterGc().entrySet()) {
            String name = entry.getKey();
            if (!ignoredMemoryAreas.contains(name)) {
              MemoryUsage detail = entry.getValue();
              totalMemUsed += detail.getUsed();
              totalMemMax += detail.getMax();
            }
          }
          float heapUsage = (float) totalMemUsed / (float) totalMemMax;
          int heapUsagePercent = (int) Math.floor(heapUsage * 100f);
          logger.trace("heap usage after GC: " + heapUsagePercent + "%");
          maybeClearReferences(heapUsage);
        }
      };
  }


  /** writes all references to disk overflow, blocks until complete.
   * useful when saving the graph */
  @Override
  public void clearAllReferences() {
    while (!clearableRefs.isEmpty()) {
      int clearableRefsSize = clearableRefs.size();
      logger.info("clearing " + clearableRefsSize + " references - this may take some time");
      for (Future clearRefFuture : asynchronouslyClearReferences(clearableRefsSize)) {
        try {
          // block until everything is cleared
          clearRefFuture.get();
        } catch (Exception e) {
          throw new RuntimeException("error while clearing references to disk", e);
        }
      }
    }
  }

  protected void uninstallGCMonitoring() {
    while (!gcNotificationListeners.isEmpty()) {
      Map.Entry<NotificationEmitter, NotificationListener> entry = gcNotificationListeners.entrySet().iterator().next();
      try {
        entry.getKey().removeNotificationListener(entry.getValue());
        gcNotificationListeners.remove(entry.getKey());
      } catch (ListenerNotFoundException e) {
        throw new RuntimeException("unable to remove GC monitor", e);
      }
    }
    logger.info("uninstalled GC monitors.");
  }

  @Override
  public void close() {
    uninstallGCMonitoring();
    executorService.shutdown();
  }

}
