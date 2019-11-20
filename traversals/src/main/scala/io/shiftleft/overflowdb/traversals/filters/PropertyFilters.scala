package io.shiftleft.overflowdb.traversals.filters

import io.shiftleft.overflowdb.traversals.Traversal

trait PropertyFilters {
  def filterExact[NodeType, Value](trav: Traversal[NodeType])(accessor: NodeType => Value, value: Value): Traversal[NodeType] =
    trav.filter(accessor(_) == value)

  def filterExactMultiple[NodeType, Value](trav: Traversal[NodeType])(accessor: NodeType => Value, values: Seq[Value]): Traversal[NodeType] = {
    val valuesSet: Set[Value] = values.to(Set)
    trav.filter(node => valuesSet.contains(accessor(node)))
  }
}