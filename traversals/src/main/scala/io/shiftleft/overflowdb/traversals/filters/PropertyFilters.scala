package io.shiftleft.overflowdb.traversals.filters

import io.shiftleft.overflowdb.traversals.Traversal

trait PropertyFilters {
  def filterExact[Trav <: Traversal[NodeType], NodeType, Value](trav: Trav, accessor: NodeType => Value, value: Value): Traversal[NodeType] =
    trav.filter(node => accessor(node) == value)

  def filterExactMultiple[Trav <: Traversal[NodeType], NodeType, Value](trav: Trav, accessor: NodeType => Value, values: Seq[Value]): Traversal[NodeType] = {
    val valuesSet: Set[Value] = values.to(Set)
    trav.filter(node => valuesSet.contains(accessor(node)))
  }
}