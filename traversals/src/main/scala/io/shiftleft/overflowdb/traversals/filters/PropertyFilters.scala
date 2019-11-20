package io.shiftleft.overflowdb.traversals.filters

import io.shiftleft.overflowdb.traversals.Traversal

object PropertyFilters {
  def exact[NodeType, Value](trav: Traversal[NodeType])(accessor: NodeType => Value, value: Value): Traversal[NodeType] =
    trav.filter(accessor(_) == value)

  def exactMultiple[NodeType, Value](trav: Traversal[NodeType])(accessor: NodeType => Value, values: Seq[Value]): Traversal[NodeType] = {
    val valuesSet: Set[Value] = values.to(Set)
    trav.filter(node => valuesSet.contains(accessor(node)))
  }
}