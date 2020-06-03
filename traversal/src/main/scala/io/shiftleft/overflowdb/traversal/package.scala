package io.shiftleft.overflowdb

import io.shiftleft.overflowdb.util.JIteratorCastingWrapper
import scala.collection.IterableOnce
import scala.jdk.CollectionConverters._

package object traversal {

  implicit def jIteratortoTraversal[A](jiterator: java.util.Iterator[A]): Traversal[A] =
    iteratorToTraversal(jiterator.asScala)

  implicit def iteratorToTraversal[A](iterator: Iterator[A]): Traversal[A] =
    iterator.to(Traversal)

  implicit def toNodeTraversal[A <: NodeRef[_]](traversal: Traversal[A]): NodeTraversal[A] =
    new NodeTraversal[A](traversal)

  implicit def toEdgeTraversal[A <: OdbEdge](traversal: Traversal[A]): EdgeTraversal[A] =
    new EdgeTraversal[A](traversal)

  implicit def toElementTraversal[A <: OdbElement](traversal: Traversal[A]): ElementTraversal[A] =
    new ElementTraversal[A](traversal)

  implicit def toNodeTraversalViaAdditionalImplicit[A <: NodeRef[_], Trav](traversable: Trav)(implicit toTraversal: Trav => Traversal[A]): NodeTraversal[A] =
    new NodeTraversal[A](toTraversal(traversable))

  implicit def toEdgeTraversalViaAdditionalImplicit[A <: OdbEdge, Trav](traversable: Trav)(implicit toTraversal: Trav => Traversal[A]): EdgeTraversal[A] =
    new EdgeTraversal[A](toTraversal(traversable))

  implicit def toElementTraversalViaAdditionalImplicit[A <: OdbElement, Trav](traversable: Trav)(implicit toTraversal: Trav => Traversal[A]): ElementTraversal[A] =
    new ElementTraversal[A](toTraversal(traversable))

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
