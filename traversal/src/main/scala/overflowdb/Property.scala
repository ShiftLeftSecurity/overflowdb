package overflowdb

case class PropertyKey[A](name: String) {
  def ->(value: A): Property[A] = Property(this, value)

  def of(value: A): Property[A] = Property(this, value)
}

case class Property[A](key: PropertyKey[A], value: A)

object Property {
  def apply[A](key: String, value: A): Property[A] =
    Property[A](PropertyKey[A](key), value)
}
