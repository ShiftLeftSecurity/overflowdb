package io.shiftleft.overflowdb.traversals

import io.shiftleft.overflowdb.NodeRef

class NodeTraversal[+A <: NodeRef[_]](traversal: Traversal[A]) {
  def id: Traversal[Long] = traversal.map(_.id)
  def label: Traversal[String] = traversal.map(_.label)
}
