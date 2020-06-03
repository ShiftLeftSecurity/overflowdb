package io.shiftleft.overflowdb.traversal

import io.shiftleft.overflowdb.{OdbElement, PropertyKey, PropertyKeyValue}

class ElementTraversal[E <: OdbElement](val traversal: Traversal[E]) extends AnyVal {

  def has(key: PropertyKey[_]): Traversal[E] = has(key.name)

  def has(name: String): Traversal[E] =
    traversal.filter(_.property2(name) != null)

  def has[P](keyValue: PropertyKeyValue[P]): Traversal[E] =
    has[P](keyValue.key, keyValue.value)

  def has[P](key: PropertyKey[P], value: P): Traversal[E] =
    traversal.filter(_.property2(key.name) == value)

  def property[P](propertyKey: PropertyKey[P]): Traversal[P] =
    property(propertyKey.name)

  def property[P](propertyKey: String): Traversal[P] =
    traversal.map(_.property2[P](propertyKey))

}
