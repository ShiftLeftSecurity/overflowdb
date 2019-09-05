package io.shiftleft.overflowdb

package object traversals {
  implicit def traversalToNodeTraversal[A <: NodeRef[_]](traversal: Traversal[A]): NodeTraversal[A] =
    new NodeTraversal[A](traversal)
}
