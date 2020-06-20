package overflowdb.traversal

import overflowdb.traversal.help.Doc
import overflowdb.{OdbElement, PropertyKey, Property}
import scala.jdk.CollectionConverters._

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

  def has[P](keyValue: Property[P]): Traversal[E] =
    has[P](keyValue.key, keyValue.value)

  def has[P](key: PropertyKey[P], value: P): Traversal[E] =
    traversal.filter(_.property2(key.name) == value)

  def hasNot(key: PropertyKey[_]): Traversal[E] = hasNot(key.name)

  def hasNot(name: String): Traversal[E] =
    traversal.filter(_.property2(name) == null)

  def hasNot[P](keyValue: Property[P]): Traversal[E] =
    hasNot[P](keyValue.key, keyValue.value)

  def hasNot[P](key: PropertyKey[P], value: P): Traversal[E] =
    traversal.filter(_.property2(key.name) != value)

  def property[P](key: PropertyKey[P]): Traversal[P] =
    property(key.name)

  def property[P](key: String): Traversal[P] =
    traversal.map(_.property2[P](key)).filter(_ != null)

  def propertyOption[P](key: PropertyKey[P]): Traversal[Option[P]] =
    propertyOption(key.name)

  def propertyOption[P](key: String): Traversal[Option[P]] =
    traversal.map(element => Option(element.property2[P](key)))

  def propertyMap: Traversal[Map[String, Object]] =
    traversal.map(_.propertyMap.asScala.toMap)

//  def repeat2(repeatTraversal: Traversal[E] => Traversal[E]): Traversal[E] = ???
}
