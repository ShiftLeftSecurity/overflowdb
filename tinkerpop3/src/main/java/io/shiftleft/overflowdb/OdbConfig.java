package io.shiftleft.overflowdb;

import java.util.Optional;

public class OdbConfig {
  private boolean overflowEnabled = true;
  private int heapPercentageThreshold = 80;
  private Optional<String> storageLocation = Optional.empty();

  public static OdbConfig withDefaults() {
    return new OdbConfig();
  }

  public static OdbConfig withoutOverflow() {
    return withDefaults().disableOverflow();
  }

  public OdbConfig disableOverflow() {
    this.overflowEnabled = false;
    return this;
  }

  /**
   * when heap - after full GC - is above this threshold, OdbGraph will start to clear some references,
   * i.e. write them to storage and set them to `null`.
   * defaults to 80, i.e. 80%
   */
  public OdbConfig withHeapPercentageThreshold(int threshold) {
    this.heapPercentageThreshold = threshold;
    return this;
  }

  /* If specified, OdbGraph will be saved there on `close`.
   * To load from that location, just instantiate a new OdbGraph with the same location. */
  public OdbConfig withStorageLocation(String path) {
    this.storageLocation = Optional.ofNullable(path);
    return this;
  }

  public boolean isOverflowEnabled() {
    return overflowEnabled;
  }

  public int getHeapPercentageThreshold() {
    return heapPercentageThreshold;
  }

  public Optional<String> getStorageLocation() {
    return storageLocation;
  }
}
