package overflowdb.traversal.filter

object PropertyFilter {
  def exact[NodeType, Value](
      trav: Iterator[NodeType]
  )(accessor: NodeType => Value, value: Value): Iterator[NodeType] =
    trav.filter(accessor(_) == value)

  def exactMultiple[NodeType, Value](
      trav: Iterator[NodeType]
  )(accessor: NodeType => Value, values: Seq[Value]): Iterator[NodeType] = {
    val valuesSet: Set[Value] = values.to(Set)
    trav.filter(node => valuesSet.contains(accessor(node)))
  }
}
