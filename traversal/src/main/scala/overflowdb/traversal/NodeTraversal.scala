package overflowdb.traversal

import overflowdb.traversal.help.Doc
import overflowdb.{Edge, Node}

import scala.jdk.CollectionConverters.IteratorHasAsScala

class NodeTraversal[E <: Node](val traversal: Iterator[E]) extends AnyVal {
  type Traversal[A] = Iterator[A]
  import ImplicitsTmp._
  @Doc(info = "Traverse to node id")
  def id: Traversal[Long] = traversal.map(_.id)

  /** Filter by given id Note: do not use as the first step in a traversal, e.g. `traversalSource.all.id(value)`. Use
    * `traversalSource.withId` instead, it is much faster
    */
  def id(value: Long): Traversal[E] =
    traversal.filter(_.id == value)

  /** Filter by given ids Note: do not use as the first step in a traversal, e.g. `traversalSource.all.id(value)`. Use
    * `traversalSource.withId` instead, it is much faster
    */
  def id(values: Long*): Traversal[E] = {
    val wanted = values.toSet
    traversal.filter(element => wanted.contains(element.id))
  }

  /** alias for {{{id}}} */
  def hasId(value: Long): Traversal[E] = id(value)

  /** alias for {{{id}}} */
  def hasId(values: Long*): Traversal[E] = id(values: _*)

  /** follow outgoing edges to adjacent nodes */
  @Doc(info = "follow outgoing edges to adjacent nodes")
  def out: Traversal[Node] =
    traversal.flatMap(_.out.asScala)

  /** follow outgoing edges of given labels to adjacent nodes */
  def out(labels: String*): Traversal[Node] =
    traversal.flatMap(_.out(labels: _*).asScala)

  /** follow incoming edges to adjacent nodes */
  def in: Traversal[Node] =
    traversal.flatMap(_.in.asScala)

  /** follow incoming edges of given label to adjacent nodes */
  def in(labels: String*): Traversal[Node] =
    traversal.flatMap(_.in(labels: _*).asScala)

  /** follow incoming and outgoing edges to adjacent nodes */
  def both: Traversal[Node] =
    traversal.flatMap(_.both.asScala)

  /** follow incoming and outgoing edges of given labels to adjacent nodes */
  def both(labels: String*): Traversal[Node] =
    traversal.flatMap(_.both(labels: _*).asScala)

  /** follow outgoing edges */
  def outE: Traversal[Edge] =
    traversal.flatMap(_.outE.asScala)

  /** follow outgoing edges of given label */
  def outE(labels: String*): Traversal[Edge] =
    traversal.flatMap(_.outE(labels: _*).asScala)

  /** follow incoming edges */
  def inE: Traversal[Edge] =
    traversal.flatMap(_.inE.asScala)

  /** follow incoming edges of given label */
  def inE(labels: String*): Traversal[Edge] =
    traversal.flatMap(_.inE(labels: _*).asScala)

  /** follow incoming and outgoing edges */
  def bothE: Traversal[Edge] =
    traversal.flatMap(_.bothE.asScala)

  /** follow incoming and outgoing edges of given label */
  def bothE(labels: String*): Traversal[Edge] =
    traversal.flatMap(_.bothE(labels: _*).asScala)

}
