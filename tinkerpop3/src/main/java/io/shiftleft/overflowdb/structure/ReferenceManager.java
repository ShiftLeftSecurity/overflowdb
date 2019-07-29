package io.shiftleft.overflowdb.structure;

public interface ReferenceManager {
  void registerRef(NodeRef ref);

  void applyBackpressureMaybe();

  void close();

  void clearAllReferences();
}
