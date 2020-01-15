package io.shiftleft.overflowdb

import io.shiftleft.overflowdb.util.JIteratorCastingWrapper
import org.apache.tinkerpop.gremlin.structure.Direction
import scala.collection.IterableOnce

package object traversal {

  implicit def toNodeTraversal[A <: NodeRef[_]](traversal: Traversal[A]): NodeTraversal[A] =
    new NodeTraversal[A](traversal)

  implicit def toEdgeTraversal[A <: OdbEdge](traversal: Traversal[A]): EdgeTraversal[A] =
    new EdgeTraversal[A](traversal)

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
