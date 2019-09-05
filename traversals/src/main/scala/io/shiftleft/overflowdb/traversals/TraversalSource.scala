package io.shiftleft.overflowdb.traversals

import io.shiftleft.overflowdb.{NodeRef, OdbGraph}

import scala.jdk.CollectionConverters._

abstract class TraversalSource(graph: OdbGraph) {
  // TODO remove map/cast step once we use our own core api
  def all: Traversal[NodeRef[_]] =
    new Traversal(graph.vertices().asScala).cast[NodeRef[_]]

  def withId(id: Long): Traversal[NodeRef[_]] =
    new Traversal(graph.vertices(id).asScala).cast[NodeRef[_]]

  def withIdTyped[A <: NodeRef[_]](id: Long): Traversal[A] =
    new Traversal(graph.vertices(id).asScala).cast[A]

  def withIds(ids: Long*): Traversal[NodeRef[_]] =
    new Traversal(graph.vertices(ids: _*).asScala).cast[NodeRef[_]]

  def withLabel(label: String): Traversal[NodeRef[_]] =
    new Traversal(graph.nodesByLabel(label).asScala)

  def withLabelTyped[A <: NodeRef[_]](label: String): Traversal[A] =
    withLabel(label).cast[A]
}