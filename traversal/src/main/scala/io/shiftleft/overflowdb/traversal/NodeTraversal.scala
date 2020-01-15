package io.shiftleft.overflowdb.traversal

import io.shiftleft.overflowdb.{NodeRef, OdbEdge}
import org.apache.tinkerpop.gremlin.structure.Direction

class NodeTraversal[A <: NodeRef[_]](val traversal: Traversal[A]) extends AnyVal {
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

  def out: Traversal[NodeRef[_]] =
    traversal.flatMap(_.vertices(Direction.OUT).toScalaAs)

  def out(label: String): Traversal[NodeRef[_]] =
    traversal.flatMap(_.vertices(Direction.OUT, label).toScalaAs)

  def outE: Traversal[OdbEdge] =
    traversal.flatMap(_.edges(Direction.OUT).toScalaAs)

  def outE(label: String): Traversal[OdbEdge] =
    traversal.flatMap(_.edges(Direction.OUT, label).toScalaAs)
}
