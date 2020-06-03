package io.shiftleft.overflowdb.traversal

import io.shiftleft.overflowdb.traversal.help.Doc
import io.shiftleft.overflowdb.{NodeRef, OdbEdge, PropertyKey, PropertyKeyValue}
import org.apache.tinkerpop.gremlin.structure.Direction

class NodeTraversal[E <: NodeRef[_]](val traversal: Traversal[E]) extends AnyVal {

  @Doc("Traverse to node id")
  def id: Traversal[Long] = traversal.map(_.id)

  @Doc("Traverse to element label")
  def label: Traversal[String] = traversal.map(_.label)

  /** Note: do not use as the first step in a traversal, e.g. `traversalSource.all.label(value)`.
   * Use `traversalSource.withLabel` instead, it is much faster */
  def label(value: String): Traversal[E] =
    traversal.filter(_.label == value)

  /** Note: do not use as the first step in a traversal, e.g. `traversalSource.all.id(value)`.
   * Use `traversalSource.withId` instead, it is much faster */
  def id(value: Long): Traversal[E] =
    traversal.filter(_.id == value)

  /** follow outgoing edges to adjacent nodes */
  def out: Traversal[NodeRef[_]] =
    traversal.flatMap(_.vertices(Direction.OUT).toScalaAs)

  /** follow outgoing edges of given label to adjacent nodes */
  def out(label: String): Traversal[NodeRef[_]] =
    traversal.flatMap(_.out(label).toScalaAs)

  /** follow incoming edges to adjacent nodes */
  def in: Traversal[NodeRef[_]] =
    traversal.flatMap(_.vertices(Direction.IN).toScalaAs)

  /** follow incoming edges of given label to adjacent nodes */
  def in(label: String): Traversal[NodeRef[_]] =
    traversal.flatMap(_.in(label).toScalaAs)

  /** follow incoming and outgoing edges to adjacent nodes */
  def both: Traversal[NodeRef[_]] =
    traversal.flatMap(_.nodes(Direction.BOTH).toScalaAs)

  /** follow incoming and outgoing edges of given label to adjacent nodes */
  def both(label: String): Traversal[NodeRef[_]] =
    traversal.flatMap(_.nodes(Direction.BOTH, label).toScalaAs)

  /** follow outgoing edges */
  def outE: Traversal[OdbEdge] =
    traversal.flatMap(_.edges(Direction.OUT).toScalaAs)

  /** follow outgoing edges of given label */
  def outE(label: String): Traversal[OdbEdge] =
    traversal.flatMap(_.outE(label).toScalaAs)

  /** follow incoming edges */
  def inE: Traversal[OdbEdge] =
    traversal.flatMap(_.edges(Direction.IN).toScalaAs)

  /** follow incoming edges of given label */
  def inE(label: String): Traversal[OdbEdge] =
    traversal.flatMap(_.inE(label).toScalaAs)

  /** follow incoming and outgoing edges */
  def bothE: Traversal[OdbEdge] =
    traversal.flatMap(_.edges(Direction.BOTH).toScalaAs)

  /** follow incoming and outgoing edges of given label */
  def bothE(label: String): Traversal[OdbEdge] =
    traversal.flatMap(_.edges(Direction.BOTH, label).toScalaAs)
}
