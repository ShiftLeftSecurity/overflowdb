package io.shiftleft.overflowdb.structure;

public class NoOpReferenceManager implements ReferenceManager {
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
  public void close() {
  }
}
