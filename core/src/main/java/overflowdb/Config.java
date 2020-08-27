package overflowdb;

import java.util.Optional;

public class Config {
  private boolean overflowEnabled = true;
  private int heapPercentageThreshold = 80;
  private Optional<String> storageLocation = Optional.empty();
  private boolean serializationStatsEnabled = false;

  public static Config withDefaults() {
    return new Config();
  }

  public static Config withoutOverflow() {
    return withDefaults().disableOverflow();
  }

  public Config disableOverflow() {
    this.overflowEnabled = false;
    return this;
  }

  /**
   * when heap - after full GC - is above this threshold, OdbGraph will start to clear some references,
   * i.e. write them to storage and set them to `null`.
   * defaults to 80, i.e. 80%
   */
  public Config withHeapPercentageThreshold(int threshold) {
    this.heapPercentageThreshold = threshold;
    return this;
  }

  /* If specified, OdbGraph will be saved there on `close`.
   * To load from that location, just instantiate a new OdbGraph with the same location. */
  public Config withStorageLocation(String path) {
    this.storageLocation = Optional.ofNullable(path);
    return this;
  }

  /* If specified, OdbGraph will measure and report serialization / deserialization timing averages. */
  public Config withSerializationStatsEnabled() {
    this.serializationStatsEnabled = true;
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

  public boolean isSerializationStatsEnabled() {
    return serializationStatsEnabled;
  }
}
