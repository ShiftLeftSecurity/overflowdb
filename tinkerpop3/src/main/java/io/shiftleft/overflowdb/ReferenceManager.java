package io.shiftleft.overflowdb;

public interface ReferenceManager extends AutoCloseable, HeapUsageMonitor.HeapNotificationListener {
  void registerRef(NodeRef ref);
  void applyBackpressureMaybe();
  void clearAllReferences();
  @Override
  void close();
}
