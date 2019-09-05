package io.shiftleft.overflowdb.traversals

import io.shiftleft.overflowdb.NodeRef

class NodeTraversal[+A <: NodeRef[_]](traversal: Traversal[A]) {
  def id: Traversal[Long] = traversal.map(_.id)
  def label: Traversal[String] = traversal.map(_.label)

  /** Note: do not use as the first step in a traversal, e.g. `traversalSource.all.id(value)`.
   *  Use `traversalSource.withId` instead, it is much faster */
  def id(value: Long): Traversal[A] = traversal.filter(_.id == value)

  /** Note: do not use as the first step in a traversal, e.g. `traversalSource.all.label(value)`.
   *  Use `traversalSource.withLabel` instead, it is much faster */
  def label(value: String): Traversal[A] = traversal.filter(_.label == value)
}
