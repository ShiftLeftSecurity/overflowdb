package overflowdb

case class PropertyKey[A](name: String) {
  def ->(value: A): Property[A] = Property(this, value)

  def where(predicate: A => Boolean): PropertyPredicate[A] =
    new PropertyPredicate[A](this, predicate)
}

case class Property[A](key: PropertyKey[A], value: A)

object Property {
  def apply[A](key: String, value: A): Property[A] =
    Property[A](PropertyKey[A](key), value)
}

class PropertyPredicate[A](val key: PropertyKey[A], val predicate: A => Boolean)

