package overflowdb.traversal

import overflowdb.traversal.help.Doc
import overflowdb.{OdbElement, PropertyKey, PropertyKeyValue}

class ElementTraversal[E <: OdbElement](val traversal: Traversal[E]) extends AnyVal {

  /** traverse to the element label */
  @Doc("Traverse to the element label")
  def label: Traversal[String] = traversal.map(_.label)

  /** filter by the element label
   * Note: do not use as the first step in a traversal, e.g. `traversalSource.all.label(value)`.
   * Use `traversalSource.withLabel` instead, it is much faster
   * TODO: make the above an automatic optimisation */
  def hasLabel(value: String): Traversal[E] =
    traversal.filter(_.label == value)

  def has(key: PropertyKey[_]): Traversal[E] = has(key.name)

  def has(name: String): Traversal[E] =
    traversal.filter(_.property2(name) != null)

  def has[P](keyValue: PropertyKeyValue[P]): Traversal[E] =
    has[P](keyValue.key, keyValue.value)

  def has[P](key: PropertyKey[P], value: P): Traversal[E] =
    traversal.filter(_.property2(key.name) == value)

  def hasNot(key: PropertyKey[_]): Traversal[E] = hasNot(key.name)

  def hasNot(name: String): Traversal[E] =
    traversal.filter(_.property2(name) == null)

  def hasNot[P](keyValue: PropertyKeyValue[P]): Traversal[E] =
    hasNot[P](keyValue.key, keyValue.value)

  def hasNot[P](key: PropertyKey[P], value: P): Traversal[E] =
    traversal.filter(_.property2(key.name) != value)

  def property[P](propertyKey: PropertyKey[P]): Traversal[P] =
    property(propertyKey.name)

  def property[P](propertyKey: String): Traversal[P] =
    traversal.map(_.property2[P](propertyKey)).filter(_ != null)

  def propertyOption[P](propertyKey: PropertyKey[P]): Traversal[Option[P]] =
    propertyOption(propertyKey.name)

  def propertyOption[P](propertyKey: String): Traversal[Option[P]] =
    traversal.map(element => Option(element.property2[P](propertyKey)))

}
