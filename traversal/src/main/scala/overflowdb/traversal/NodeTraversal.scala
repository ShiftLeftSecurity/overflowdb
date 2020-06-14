package overflowdb.traversal

import overflowdb.traversal.help.Doc
import overflowdb.{Node, OdbEdge}
import org.apache.tinkerpop.gremlin.structure.Direction

class NodeTraversal[E <: Node](val traversal: Traversal[E]) extends AnyVal {

  @Doc("Traverse to node id")
  def id: Traversal[Long] = traversal.map(_.id2)

  /** Note: do not use as the first step in a traversal, e.g. `traversalSource.all.id(value)`.
   * Use `traversalSource.withId` instead, it is much faster */
  def hasId(value: Long): Traversal[E] =
    traversal.filter(_.id == value)

  /** follow outgoing edges to adjacent nodes */
  @Doc("follow outgoing edges to adjacent nodes")
  def out: Traversal[Node] =
    traversal.flatMap(_.out)

  /** follow outgoing edges of given label to adjacent nodes */
  def out(label: String): Traversal[Node] =
    traversal.flatMap(_.out(label).toScalaAs)

  /** follow incoming edges to adjacent nodes */
  def in: Traversal[Node] =
    traversal.flatMap(_.in)

  /** follow incoming edges of given label to adjacent nodes */
  def in(label: String): Traversal[Node] =
    traversal.flatMap(_.in(label).toScalaAs)

  /** follow incoming and outgoing edges to adjacent nodes */
  def both: Traversal[Node] =
    traversal.flatMap(_.both)

  /** follow incoming and outgoing edges of given label to adjacent nodes */
  def both(label: String): Traversal[Node] =
    traversal.flatMap(_.both(label))

  /** follow outgoing edges */
  def outE: Traversal[OdbEdge] =
    traversal.flatMap(_.outE)

  /** follow outgoing edges of given label */
  def outE(label: String): Traversal[OdbEdge] =
    traversal.flatMap(_.outE(label))

  /** follow incoming edges */
  def inE: Traversal[OdbEdge] =
    traversal.flatMap(_.inE)

  /** follow incoming edges of given label */
  def inE(label: String): Traversal[OdbEdge] =
    traversal.flatMap(_.inE(label))

  /** follow incoming and outgoing edges */
  def bothE: Traversal[OdbEdge] =
    traversal.flatMap(_.bothE)

  /** follow incoming and outgoing edges of given label */
  def bothE(label: String): Traversal[OdbEdge] =
    traversal.flatMap(_.bothE(label))

}
