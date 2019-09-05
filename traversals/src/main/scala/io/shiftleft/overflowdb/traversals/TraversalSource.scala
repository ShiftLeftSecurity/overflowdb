package io.shiftleft.overflowdb.traversals

import io.shiftleft.overflowdb.{NodeRef, OdbGraph}

import scala.jdk.CollectionConverters._

abstract class TraversalSource(graph: OdbGraph) {
  // TODO remove map/cast step once we use our own core api
  def all: Traversal[NodeRef[_]] = new Traversal(graph.vertices().asScala.map(_.asInstanceOf[NodeRef[_]]))

  protected def nodesByLabel(label: String): Traversal[NodeRef[_]] =
    new Traversal(graph.nodesByLabel(label).asScala)

  protected def nodesByLabelTyped[A <: NodeRef[_]](label: String): Traversal[A] =
    nodesByLabel(label).map(_.asInstanceOf[A])
}