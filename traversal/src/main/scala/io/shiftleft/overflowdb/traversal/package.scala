package io.shiftleft.overflowdb

import io.shiftleft.overflowdb.util.JIteratorCastingWrapper
import org.apache.tinkerpop.gremlin.structure.Direction
import scala.collection.IterableOnce

package object traversal {

  implicit class NodeTraversal[A <: NodeRef[_]](val traversal: Traversal[A]) extends AnyVal {
    def id: Traversal[Long] = traversal.map(_.id)

    def label: Traversal[String] = traversal.map(_.label)

    def property[P](name: String): Traversal[P] =
      traversal.map(_.value[P](name))

    def property[P](propertyKey: PropertyKey[P]): Traversal[P] =
      traversal.map(_.value[P](propertyKey.name))

    def hasProperty(name: String): Traversal[A] =
      traversal.filter(_.property(name).isPresent)

    def hasProperty(key: PropertyKey[_]): Traversal[A] = hasProperty(key.name)

    def hasProperty[P](keyValue: PropertyKeyValue[P]): Traversal[A] =
      hasProperty[P](keyValue.key, keyValue.value)

    def hasProperty[P](key: PropertyKey[P], value: P): Traversal[A] =
      traversal.filter { node =>
        val property = node.property[P](key.name)
        property.isPresent && property.value == value
      }

    /** Note: do not use as the first step in a traversal, e.g. `traversalSource.all.id(value)`.
      * Use `traversalSource.withId` instead, it is much faster */
    def id(value: Long): Traversal[A] = traversal.filter(_.id == value)

    /** Note: do not use as the first step in a traversal, e.g. `traversalSource.all.label(value)`.
      * Use `traversalSource.withLabel` instead, it is much faster */
    def label(value: String): Traversal[A] = traversal.filter(_.label == value)

    def out[B <: NodeRef[_]]: Traversal[B] =
      traversal.flatMap(_.vertices(Direction.OUT).toScalaAs[B])

    def out[B <: NodeRef[_]](label: String): Traversal[B] =
      traversal.flatMap(_.vertices(Direction.OUT, label).toScalaAs[B])

    def outE[B <: OdbEdge]: Traversal[B] =
      traversal.flatMap(_.edges(Direction.OUT).toScalaAs[B])

    def outE[B <: OdbEdge](label: String): Traversal[B] =
      traversal.flatMap(_.edges(Direction.OUT, label).toScalaAs[B])
  }

  implicit class EdgeTraversal[A <: OdbEdge](val traversal: Traversal[A]) extends AnyVal {
    def inV[B <: NodeRef[_]]: Traversal[B] =
      traversal.iterator.map(_.inVertex().asInstanceOf[B]).to(Traversal)
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
