package overflowdb

import overflowdb.util.JIteratorCastingWrapper

import scala.collection.IterableOnce
import scala.jdk.CollectionConverters._

package object traversal {

  implicit def jIteratortoTraversal[A](jiterator: java.util.Iterator[A]): Traversal[A] =
    iteratorToTraversal(jiterator.asScala)

  implicit def iteratorToTraversal[A](iterator: Iterator[A]): Traversal[A] =
    iterator.to(Traversal)

  implicit def iterableToTraversal[A](iterable: IterableOnce[A]): Traversal[A] =
    Traversal.from(iterable)

  implicit def toNodeTraversal[A <: Node](traversal: Traversal[A]): NodeTraversal[A] =
    new NodeTraversal[A](traversal)

  implicit def toEdgeTraversal[A <: Edge](traversal: Traversal[A]): EdgeTraversal[A] =
    new EdgeTraversal[A](traversal)

  implicit def toElementTraversal[A <: Element](traversal: Traversal[A]): ElementTraversal[A] =
    new ElementTraversal[A](traversal)

  implicit def toNodeTraversalViaAdditionalImplicit[A <: Node, Trav](traversable: Trav)(implicit toTraversal: Trav => Traversal[A]): NodeTraversal[A] =
    new NodeTraversal[A](toTraversal(traversable))

  implicit def toEdgeTraversalViaAdditionalImplicit[A <: Edge, Trav](traversable: Trav)(implicit toTraversal: Trav => Traversal[A]): EdgeTraversal[A] =
    new EdgeTraversal[A](toTraversal(traversable))

  implicit def toElementTraversalViaAdditionalImplicit[A <: Element, Trav](traversable: Trav)(implicit toTraversal: Trav => Traversal[A]): ElementTraversal[A] =
    new ElementTraversal[A](toTraversal(traversable))

  implicit def toNumericTraversal[A : Numeric](traversal: Traversal[A]): NumericTraversal[A] =
    new NumericTraversal[A](traversal)

  implicit class NodeOps[N <: Node](val node: N) extends AnyVal {
    /** start a new Traversal with this Node, i.e. lift it into a Traversal */
    def start: Traversal[N] =
      Traversal.fromSingle(node)
  }

  implicit class JIterableOps[A](val jIterator: java.util.Iterator[A]) extends AnyVal {

    /**
      * Wraps a java iterator into a scala iterator, and casts it's elements.
      * This is faster than `jIterator.asScala.map(_.asInstanceOf[B])` because
      * 1) `.asScala` conversion is actually quite slow: multiple method calls and a `match` without @switch
      * 2) no additional `map` step that iterates and creates yet another iterator
     **/
    def toScalaAs[B]: IterableOnce[B] =
      new JIteratorCastingWrapper[B](jIterator)
  }

}
