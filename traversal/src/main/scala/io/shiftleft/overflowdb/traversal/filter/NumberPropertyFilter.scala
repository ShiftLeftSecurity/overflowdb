package io.shiftleft.overflowdb.traversal.filter

import io.shiftleft.overflowdb.traversal.Traversal

/** to avoid boxing conversion at runtime, the implementation is replicated for each number type */
object NumberPropertyFilter {

  object Int {
    def gt[NodeType](trav: Traversal[NodeType])(accessor: NodeType => Int,
                                                value: Int): Traversal[NodeType] =
      trav.filter(node => accessor(node) > value)

    def gte[NodeType](trav: Traversal[NodeType])(accessor: NodeType => Int,
                                                 value: Int): Traversal[NodeType] =
      trav.filter(node => accessor(node) >= value)

    def lt[NodeType](trav: Traversal[NodeType])(accessor: NodeType => Int,
                                                value: Int): Traversal[NodeType] =
      trav.filter(node => accessor(node) < value)

    def lte[NodeType](trav: Traversal[NodeType])(accessor: NodeType => Int,
                                                 value: Int): Traversal[NodeType] =
      trav.filter(node => accessor(node) <= value)
  }

  object Long {
    def gt[NodeType](trav: Traversal[NodeType])(accessor: NodeType => Long,
                                                value: Long): Traversal[NodeType] =
      trav.filter(node => accessor(node) > value)

    def gte[NodeType](trav: Traversal[NodeType])(accessor: NodeType => Long,
                                                 value: Long): Traversal[NodeType] =
      trav.filter(node => accessor(node) >= value)

    def lt[NodeType](trav: Traversal[NodeType])(accessor: NodeType => Long,
                                                value: Long): Traversal[NodeType] =
      trav.filter(node => accessor(node) < value)

    def lte[NodeType](trav: Traversal[NodeType])(accessor: NodeType => Long,
                                                 value: Long): Traversal[NodeType] =
      trav.filter(node => accessor(node) <= value)
  }

  object Float {
    def gt[NodeType](trav: Traversal[NodeType])(accessor: NodeType => Float,
                                                value: Float): Traversal[NodeType] =
      trav.filter(node => accessor(node) > value)

    def gte[NodeType](trav: Traversal[NodeType])(accessor: NodeType => Float,
                                                 value: Float): Traversal[NodeType] =
      trav.filter(node => accessor(node) >= value)

    def lt[NodeType](trav: Traversal[NodeType])(accessor: NodeType => Float,
                                                value: Float): Traversal[NodeType] =
      trav.filter(node => accessor(node) < value)

    def lte[NodeType](trav: Traversal[NodeType])(accessor: NodeType => Float,
                                                 value: Float): Traversal[NodeType] =
      trav.filter(node => accessor(node) <= value)
  }

  object Double {
    def gt[NodeType](trav: Traversal[NodeType])(accessor: NodeType => Double,
                                                value: Double): Traversal[NodeType] =
      trav.filter(node => accessor(node) > value)

    def gte[NodeType](trav: Traversal[NodeType])(accessor: NodeType => Double,
                                                 value: Double): Traversal[NodeType] =
      trav.filter(node => accessor(node) >= value)

    def lt[NodeType](trav: Traversal[NodeType])(accessor: NodeType => Double,
                                                value: Double): Traversal[NodeType] =
      trav.filter(node => accessor(node) < value)

    def lte[NodeType](trav: Traversal[NodeType])(accessor: NodeType => Double,
                                                 value: Double): Traversal[NodeType] =
      trav.filter(node => accessor(node) <= value)
  }

}
