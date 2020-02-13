package io.shiftleft.overflowdb.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BookKeeper {
  protected final Logger logger = LoggerFactory.getLogger(getClass());
  public final boolean statsEnabled;
  private int totalCount = 0;
  private long totalTimeSpentNanos = 0;

  protected BookKeeper(boolean statsEnabled) {
    this.statsEnabled = statsEnabled;
  }

  protected final long getStartTimeNanos() {
    // System.nanoTime is relatively expensive - only  go there if we're actually recording stats
    return statsEnabled ? System.nanoTime() : 0;
  }

  protected void recordStatistics(long startTimeNanos) {
    totalCount++;
    totalTimeSpentNanos += System.nanoTime() - startTimeNanos;
    if (0 == (totalCount & 0x0001ffff)) {
      float avgSerializationTime = 1.0f-6 * totalTimeSpentNanos / (float) totalCount;
      logger.debug("stats: handled " + totalCount + " nodes in total (avg time: " + avgSerializationTime + "ms)");
    }
  }

  public final int getSerializedCount() {
    if (statsEnabled) return totalCount;
    else throw new RuntimeException("serialization statistics not enabled");
  }
}
