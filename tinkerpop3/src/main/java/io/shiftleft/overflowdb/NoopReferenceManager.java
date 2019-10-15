package io.shiftleft.overflowdb;

public class NoopReferenceManager implements ReferenceManager {
  @Override
  public void registerRef(NodeRef ref) {
  }

  @Override
  public void applyBackpressureMaybe() {
  }

  @Override
  public void clearAllReferences() {
  }

  @Override
  public void notifyHeapAboveThreshold() {
  }

  @Override
  public void close() {
  }
}
