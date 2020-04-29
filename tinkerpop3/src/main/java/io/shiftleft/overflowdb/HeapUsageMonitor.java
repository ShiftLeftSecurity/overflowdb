package io.shiftleft.overflowdb;

import com.sun.management.GarbageCollectionNotificationInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ListenerNotFoundException;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.MemoryUsage;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * watches GC activity, and when we're low on available heap space, it instructs the ReferenceManager to
 * clear some references, in order to avoid an OutOfMemoryError
 */
public class HeapUsageMonitor implements AutoCloseable {
  interface HeapNotificationListener {
    void notifyHeapAboveThreshold();
  }

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final Map<NotificationEmitter, NotificationListener> gcNotificationListeners = new HashMap<>(2);

  public HeapUsageMonitor(int heapPercentageThreshold, HeapNotificationListener notificationListener) {
    if (heapPercentageThreshold < 0 || heapPercentageThreshold > 100) {
      throw new IllegalArgumentException("heapPercentageThreshold must be between 0 and 100, but is " + heapPercentageThreshold);
    }
    float heapUsageThreshold = (float) heapPercentageThreshold / 100f;
    installGCMonitoring(heapUsageThreshold, notificationListener);
  }

  /**
   * monitor GC, and should the heap grow above 80% usage, clear some strong references
   *
   * @param heapUsageThreshold range 0.0 - 1.0
   */
  protected void installGCMonitoring(float heapUsageThreshold, HeapNotificationListener notificationListener) {
    List<GarbageCollectorMXBean> gcbeans = java.lang.management.ManagementFactory.getGarbageCollectorMXBeans();
    for (GarbageCollectorMXBean gcbean : gcbeans) {
      NotificationListener listener = createNotificationListener(heapUsageThreshold, notificationListener);
      NotificationEmitter emitter = (NotificationEmitter) gcbean;
      emitter.addNotificationListener(listener, null, null);
      gcNotificationListeners.put(emitter, listener);
    }
    int heapUsageThresholdPercent = (int) Math.floor(heapUsageThreshold * 100f);
    logger.info("installed GC monitors. will clear references if heap (after GC) is larger than " + heapUsageThresholdPercent + "%");
  }

  private NotificationListener createNotificationListener(float heapUsageThreshold, HeapNotificationListener notificationListener) {
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
        if (heapUsage > heapUsageThreshold) {
          String msg = "heap usage after GC: " + heapUsagePercent + "% -> will clear some references (if possible)";
          if (heapUsagePercent > 95) logger.warn(msg);
          else logger.info(msg);

          notificationListener.notifyHeapAboveThreshold();
        } else {
          logger.trace("heap usage after GC: " + heapUsagePercent + "%");
        }
      }
    };
  }

  public void close() {
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
}
