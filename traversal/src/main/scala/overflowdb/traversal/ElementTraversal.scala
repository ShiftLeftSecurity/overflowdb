package overflowdb.traversal

import overflowdb.traversal.help.Doc
import overflowdb.{Element, Property, PropertyPredicate, PropertyKey}

import scala.jdk.CollectionConverters._

class ElementTraversal[E <: Element](val traversal: Iterator[E]) extends AnyVal {
  type Traversal[A] = Iterator[A]

  /** traverse to the element label */
  @Doc(info = "Traverse to the element label")
  def label: Traversal[String] = traversal.map(_.label)

  /** filter by the element label Note: do not use as the first step in a traversal, e.g.
    * `traversalSource.all.label(value)`. Use `traversalSource.label` instead, it is much faster TODO: make the above an
    * automatic optimisation
    */
  def label(value: String): Traversal[E] =
    traversal.filter(_.label == value)

  /** filter by the element labels Note: do not use as the first step in a traversal, e.g.
    * `traversalSource.all.label(value)`. Use `traversalSource.label` instead, it is much faster TODO: make the above an
    * automatic optimisation
    */
  def label(values: String*): Traversal[E] = {
    val wanted = values.toSet
    traversal.filter(element => wanted.contains(element.label))
  }

  /** alias for {{{label}}} */
  def hasLabel(value: String): Traversal[E] = label(value)

  /** alias for {{{label}}} */
  def hasLabel(values: String*): Traversal[E] = label(values: _*)

  /** filter by the element label (inverse) */
  def labelNot(value: String): Traversal[E] =
    traversal.filterNot(_.label == value)

  /** filter by the element labels (inverse) */
  def labelNot(value1: String, valueN: String*): Traversal[E] = {
    val unwanted = (valueN :+ value1).toSet
    traversal.filterNot(element => unwanted.contains(element.label))
  }

  /** Filter elements by existence of property (irrespective of value) */
  def has(key: PropertyKey[_]): Traversal[E] = has(key.name)

  /** Filter elements by (non-)existence of property (irrespective of value) */
  def hasNot(key: PropertyKey[_]): Traversal[E] = hasNot(key.name)

  /** Filter elements by existence of property (irrespective of value) */
  def has(name: String): Traversal[E] =
    traversal.filter(_.property(name) != null)

  /** Filter elements by (non-)existence of property (irrespective of value) */
  def hasNot(name: String): Traversal[E] =
    traversal.filter(_.property(name) == null)

  /** Filter elements by property value */
  def has(keyValue: Property[_]): Traversal[E] =
    has(keyValue.key.name, keyValue.value)

  /** Filter elements by property value */
  def hasNot(keyValue: Property[_]): Traversal[E] =
    hasNot(keyValue.key.name, keyValue.value)

  /** Filter elements by property value */
  def has[A](key: PropertyKey[A], value: A): Traversal[E] =
    has(key.name, value)

  /** Filter elements by property value */
  def hasNot[A](key: PropertyKey[A], value: A): Traversal[E] =
    hasNot(key.name, value)

  /** Filter elements by property with given predicate.
    * @example
    *   from GenericGraphTraversalTest
    *   {{{
    * .has(Name.where(_.endsWith("1")))
    * .has(Name.where(_.matches("[LR].")))
    * .has(Name.where(P.eq("R1")))
    * .has(Name.where(P.neq("R1")))
    * .has(Name.where(P.within(Set("L1", "L2"))))
    * .has(Name.where(P.within("L1", "L2", "L3")))
    * .has(Name.where(P.matches("[LR].")))
    *   }}}
    */
  def has[A](propertyPredicate: PropertyPredicate[A]): Traversal[E] =
    traversal.filter(element => propertyPredicate.predicate(element.property(propertyPredicate.key)))

  /** Filter elements by property value */
  def has(key: String, value: Any): Traversal[E] =
    traversal.filter(_.property(key) == value)

  /** Filter elements by property value */
  def hasNot(key: String, value: Any): Traversal[E] =
    traversal.filter(_.property(key) != value)

  /** Filter elements by property with given predicate.
    * @example
    *   from GenericGraphTraversalTest
    *   {{{
    * .hasNot(Name.where(_.endsWith("1")))
    * .hasNot(Name.where(_.matches("[LR].")))
    * .hasNot(Name.where(P.eq("R1")))
    * .hasNot(Name.where(P.neq("R1")))
    * .hasNot(Name.where(P.within(Set("L1", "L2"))))
    * .hasNot(Name.where(P.within("L1", "L2", "L3")))
    * .hasNot(Name.where(P.matches("[LR].")))
    *   }}}
    */
  def hasNot[A](propertyPredicate: PropertyPredicate[A]): Traversal[E] =
    traversal.filterNot(element => propertyPredicate.predicate(element.property(propertyPredicate.key)))

  def property[A](key: PropertyKey[A]): Traversal[A] =
    property(key.name)

  def property[A](key: String): Traversal[A] =
    traversal.map(_.property(key).asInstanceOf[A]).filter(_ != null)

  def propertyOption[A](key: PropertyKey[A]): Traversal[Option[A]] =
    propertyOption(key.name)

  def propertyOption[A](key: String): Traversal[Option[A]] =
    traversal.map(element => Option(element.property(key).asInstanceOf[A]))

  def propertiesMap: Traversal[Map[String, Object]] =
    traversal.map(_.propertiesMap.asScala.toMap)

}
