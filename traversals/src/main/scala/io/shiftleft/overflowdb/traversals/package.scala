package io.shiftleft.overflowdb

import java.util

import org.apache.tinkerpop.gremlin.structure.Direction

import scala.collection.IterableOnce
import scala.jdk.CollectionConverters._

package object traversals {

  implicit class NodeTraversal[+A <: NodeRef[_]](val traversal: Traversal[A]) extends AnyVal {
    def id: Traversal[Long] = traversal.map(_.id)

    def label: Traversal[String] = traversal.map(_.label)

    def property[P](key: String): Traversal[P] = traversal.map(_.value[P](key))

    def hasProperty(key: String): Traversal[A] = traversal.filter(_.property(key).isPresent)

    def hasProperty[P](key: String, value: String): Traversal[A] =
      traversal.filter { node =>
        val property = node.property[P](key)
        property.isPresent && property.value == value
      }

    /** Note: do not use as the first step in a traversal, e.g. `traversalSource.all.id(value)`.
     * Use `traversalSource.withId` instead, it is much faster */
    def id(value: Long): Traversal[A] = traversal.filter(_.id == value)

    /** Note: do not use as the first step in a traversal, e.g. `traversalSource.all.label(value)`.
     * Use `traversalSource.withLabel` instead, it is much faster */
    def label(value: String): Traversal[A] = traversal.filter(_.label == value)

    def out[B <: NodeRef[_]](label: String): Traversal[B] =
      nodes[B](Direction.OUT, label)

    private def nodes[B <: NodeRef[_]](direction: Direction, label: String): Traversal[B] =
      traversal.flatMap(_.vertices(direction, label).cast[B].asScala)
  }

  implicit class JIterableOps[A](val iterable: java.util.Iterator[A]) extends AnyVal {
    def cast[B]: java.util.Iterator[B] = new java.util.Iterator[B] {
      override def hasNext = iterable.hasNext
      override def next() = iterable.next.asInstanceOf[B]
    }
  }

  implicit class IterableOnceOps[A](val iterable: IterableOnce[A]) extends AnyVal {
    def cast[B]: IterableOnce[B] = iterable.iterator.map(_.asInstanceOf[B])
  }

}
