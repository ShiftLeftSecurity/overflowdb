package overflowdb.traversal

import overflowdb.traversal.help.Doc
import overflowdb.{Node, OdbEdgeTp3}

class NodeTraversal[E <: Node](val traversal: Traversal[E]) extends AnyVal {

  @Doc("Traverse to node id")
  def id: Traversal[Long] = traversal.map(_.id2)

  /** Filter by given id
   * Note: do not use as the first step in a traversal, e.g. `traversalSource.all.id(value)`.
   * Use `traversalSource.withId` instead, it is much faster */
  def id(value: Long): Traversal[E] =
    traversal.filter(_.id == value)

  /** Filter by given ids
   * Note: do not use as the first step in a traversal, e.g. `traversalSource.all.id(value)`.
   * Use `traversalSource.withId` instead, it is much faster */
  def id(values: Long*): Traversal[E] = {
    val wanted = values.toSet
    traversal.filter(element => wanted.contains(element.id2))
  }

  /** alias for {{{id}}} */
  def hasId(value: Long): Traversal[E] = id(value)

  /** alias for {{{id}}} */
  def hasId(values: Long*): Traversal[E] = id(values: _*)

  /** follow outgoing edges to adjacent nodes */
  @Doc("follow outgoing edges to adjacent nodes")
  def out: Traversal[Node] =
    traversal.flatMap(_.out)

  /** follow outgoing edges of given labels to adjacent nodes */
  def out(labels: String*): Traversal[Node] =
    traversal.flatMap(_.out(labels: _*).toScalaAs)

  /** follow incoming edges to adjacent nodes */
  def in: Traversal[Node] =
    traversal.flatMap(_.in)

  /** follow incoming edges of given label to adjacent nodes */
  def in(labels: String*): Traversal[Node] =
    traversal.flatMap(_.in(labels: _*).toScalaAs)

  /** follow incoming and outgoing edges to adjacent nodes */
  def both: Traversal[Node] =
    traversal.flatMap(_.both)

  /** follow incoming and outgoing edges of given labels to adjacent nodes */
  def both(labels: String*): Traversal[Node] =
    traversal.flatMap(_.both(labels: _*))

  /** follow outgoing edges */
  def outE: Traversal[OdbEdgeTp3] =
    traversal.flatMap(_.outE)

  /** follow outgoing edges of given label */
  def outE(labels: String*): Traversal[OdbEdgeTp3] =
    traversal.flatMap(_.outE(labels: _*))

  /** follow incoming edges */
  def inE: Traversal[OdbEdgeTp3] =
    traversal.flatMap(_.inE)

  /** follow incoming edges of given label */
  def inE(labels: String*): Traversal[OdbEdgeTp3] =
    traversal.flatMap(_.inE(labels: _*))

  /** follow incoming and outgoing edges */
  def bothE: Traversal[OdbEdgeTp3] =
    traversal.flatMap(_.bothE)

  /** follow incoming and outgoing edges of given label */
  def bothE(labels: String*): Traversal[OdbEdgeTp3] =
    traversal.flatMap(_.bothE(labels: _*))

}
