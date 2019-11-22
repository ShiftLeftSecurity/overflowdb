package io.shiftleft.overflowdb

import io.shiftleft.overflowdb.util.JIteratorCastingWrapper
import org.apache.tinkerpop.gremlin.structure.Direction
import scala.collection.IterableOnce

package object traversal {

  implicit class NodeTraversal[+A <: NodeRef[_]](val traversal: Traversal[A]) extends AnyVal {
    def id: Traversal[Long] = traversal.map(_.id)

    def label: Traversal[String] = traversal.map(_.label)

    def property[A](name: String): Traversal[A] = traversal.map(_.value[A](name))

    def property[A](propertyKey: PropertyKey[A]): Traversal[A] = traversal.map(_.value[A](propertyKey.name))

    def hasProperty(name: String): Traversal[A] = traversal.filter(_.property(name).isPresent)

    def hasProperty(key: PropertyKey[_]): Traversal[A] = hasProperty(key.name)

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
      traversal.flatMap(_.vertices(direction, label).toScalaAs[B])
  }

  implicit class JIterableOps[A](val jIterator: java.util.Iterator[A]) extends AnyVal {
    /**
     * Wraps a java iterator into a scala iterator, and casts it's elements.
     * This is faster than `jIterator.asScala.map(_.asInstanceOf[B])` because
     * 1) `.asScala` conversion is actually quite slow: multiple method calls and a `match` without @switch
     * 2) no additional `map` step that iterates and creates yet another iterator
     **/
    def toScalaAs[B]: IterableOnce[B] = new JIteratorCastingWrapper[B](jIterator)
  }

}
