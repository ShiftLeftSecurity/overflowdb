package io.shiftleft.overflowdb.traversal

import io.shiftleft.overflowdb.{OdbElement, PropertyKey}

class ElementTraversal[E <: OdbElement](val traversal: Traversal[E]) extends AnyVal {

  def property[P](propertyKey: PropertyKey[P]): Traversal[P] =
    property(propertyKey.name)

  def property[P](propertyKey: String): Traversal[P] =
    traversal.map(_.property2[P](propertyKey))


}
