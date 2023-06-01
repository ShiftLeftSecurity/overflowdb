package overflowdb.traversal

import overflowdb.{Edge, Element, Node}
import scala.jdk.CollectionConverters._

trait Implicits {
  type Traversal[+A] = Iterator[A]
  implicit def jIteratortoTraversal[A](jiterator: java.util.Iterator[A]): Iterator[A] = jiterator.asScala

  implicit def toTraversalSugarExt[A](iter: Iterator[A]): TraversalSugarExt[A] = new TraversalSugarExt(iter)
  implicit def toTraversalLogicExt[A](iter: Iterator[A]): TraversalLogicExt[A] = new TraversalLogicExt(iter)
  implicit def toTraversalFilterExt[A](iter: Iterator[A]): TraversalFilterExt[A] = new TraversalFilterExt(iter)

  implicit def toTraversalTrackingExt[A](iter: Iterator[A]): TraversalTrackingExt[A] = new TraversalTrackingExt(iter)
  implicit def toRepeatTraversalExt[A](iter: Iterator[A]): TraversalRepeatExt[A] = new TraversalRepeatExt(iter)

  implicit def iterableToTraversal[A](iterable: IterableOnce[A]): Iterator[A] = iterable.iterator
  implicit def toNodeTraversal[A <: Node](traversal: Iterator[A]): NodeTraversal[A] =
    new NodeTraversal[A](traversal)

  implicit def toEdgeTraversal[A <: Edge](traversal: Iterator[A]): EdgeTraversal[A] =
    new EdgeTraversal[A](traversal)

  implicit def toElementTraversal[A <: Element](traversal: Iterator[A]): ElementTraversal[A] =
    new ElementTraversal[A](traversal)

  implicit def toNodeOps[N <: Node](node: N): NodeOps[N] = new NodeOps(node)

  // TODO make available again once we're on Scala 3.3.0
  // context: these break REPL autocompletion, e.g. in joern for `cpg.<tab>`
  // fixed via https://github.com/lampepfl/dotty/issues/16360#issuecomment-1324857836

  // implicit def toNodeTraversalViaAdditionalImplicit[A <: Node, Trav](traversable: Trav)(implicit toTraversal: Trav => Traversal[A]): NodeTraversal[A] =
  //   new NodeTraversal[A](toTraversal(traversable))

  // implicit def toEdgeTraversalViaAdditionalImplicit[A <: Edge, Trav](traversable: Trav)(implicit toTraversal: Trav => Traversal[A]): EdgeTraversal[A] =
  //   new EdgeTraversal[A](toTraversal(traversable))

  // implicit def toElementTraversalViaAdditionalImplicit[A <: Element, Trav](traversable: Trav)(implicit toTraversal: Trav => Traversal[A]): ElementTraversal[A] =
  //   new ElementTraversal[A](toTraversal(traversable))

  // implicit def toNumericTraversal[A : Numeric](traversal: Traversal[A]): NumericTraversal[A] =
  //   new NumericTraversal[A](traversal)

}

class NodeOps[N <: Node](val node: N) extends AnyVal {

  /** start a new Traversal with this Node, i.e. lift it into a Traversal. Will hopefully be deprecated in favor of
    * "iterator"
    */
  def start: Iterator[N] =
    Iterator.single(node)

  /** start a new Traversal with this Node, i.e. lift it into a Traversal */
  def iterator: Iterator[N] = Iterator.single(node)
}

object ImplicitsTmp extends Implicits
