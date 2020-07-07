package overflowdb.traversal

import overflowdb.traversal.filter.P
import overflowdb.traversal.help.Doc
import overflowdb.{OdbElement, Property, PropertyPredicate, PropertyKey}

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

  /** Filter elements by existence of property (irrespective of value) */
  def has(key: PropertyKey[_]): Traversal[E] = has(key.name)

  /** Filter elements by existence of property (irrespective of value) */
  def has(name: String): Traversal[E] =
    traversal.filter(_.property2(name) != null)

  /** Filter elements by property value */
  def has[A](keyValue: Property[A]): Traversal[E] =
    has[A](keyValue.key, keyValue.value)

  /** Filter elements by property value */
  def has[A](key: PropertyKey[A], value: A): Traversal[E] =
    traversal.filter(_.property2(key.name) == value)

  /** Filter elements by property with given predicate.
   * @example from GenericGraphTraversalTest
   * {{{
   * .has(Name.where(_.endsWith("1")))
   * .has(Name.where(_.matches("[LR].")))
   * .has(Name.where(P.eq("R1")))
   * .has(Name.where(P.neq("R1")))
   * .has(Name.where(P.within(Set("L1", "L2"))))
   * .has(Name.where(P.within("L1", "L2", "L3")))
   * .has(Name.where(P.matches("[LR].")))
   * }}}
   */
  def has[A](propertyPredicate: PropertyPredicate[A]): Traversal[E] =
    traversal.filter(element => propertyPredicate.predicate(element.property(propertyPredicate.key)))

  def hasNot(key: PropertyKey[_]): Traversal[E] = hasNot(key.name)

  def hasNot(name: String): Traversal[E] =
    traversal.filter(_.property2(name) == null)

  def hasNot[A](keyValue: Property[A]): Traversal[E] =
    hasNot[A](keyValue.key, keyValue.value)

  def hasNot[A](key: PropertyKey[A], value: A): Traversal[E] =
    traversal.filter(_.property2(key.name) != value)

  def hasNot[A](key: PropertyKey[A], predicate: A => Boolean): Traversal[E] =
    traversal.filter(element => predicate(element.property2(key.name)))

  def property[A](key: PropertyKey[A]): Traversal[A] =
    property(key.name)

  def property[A](key: String): Traversal[A] =
    traversal.map(_.property2[A](key)).filter(_ != null)

  def propertyOption[A](key: PropertyKey[A]): Traversal[Option[A]] =
    propertyOption(key.name)

  def propertyOption[A](key: String): Traversal[Option[A]] =
    traversal.map(element => Option(element.property2[A](key)))

  def propertyMap: Traversal[Map[String, Object]] =
    traversal.map(_.propertyMap.asScala.toMap)

}
