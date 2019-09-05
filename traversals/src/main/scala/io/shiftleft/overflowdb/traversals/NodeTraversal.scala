package io.shiftleft.overflowdb.traversals

import io.shiftleft.overflowdb.NodeRef

class NodeTraversal[+A <: NodeRef[_]](traversal: Traversal[A]) {
  def id: Traversal[Long] = traversal.map(_.id)
  def id(value: Long): Traversal[A] = traversal.filter(_.id == value)

  def label: Traversal[String] = traversal.map(_.label)
  def label(value: String): Traversal[A] = traversal.filter(_.label == value)
}
