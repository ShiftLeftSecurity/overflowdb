package io.shiftleft.overflowdb.traversals

import io.shiftleft.overflowdb.{NodeRef, OdbGraph}

// TODO stop using `cast` step once we have our own core api
abstract class TraversalSource(graph: OdbGraph) {
  def all: Traversal[NodeRef[_]] =
    Traversal(graph.vertices()).cast[NodeRef[_]]

  def id(id: Long): Traversal[NodeRef[_]] =
    Traversal(graph.vertices(id)).cast[NodeRef[_]]

  def idTyped[A <: NodeRef[_]](id: Long): Traversal[A] =
    Traversal(graph.vertices(id)).cast[A]

  def ids(ids: Long*): Traversal[NodeRef[_]] =
    Traversal(graph.vertices(ids: _*)).cast[NodeRef[_]]

  def label(label: String): Traversal[NodeRef[_]] =
    Traversal(graph.nodesByLabel(label))

  def labelTyped[A <: NodeRef[_]](value: String): Traversal[A] =
    label(value).cast[A]
}