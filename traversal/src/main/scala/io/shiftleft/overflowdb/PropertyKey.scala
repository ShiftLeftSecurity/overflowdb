package io.shiftleft.overflowdb

case class PropertyKey[A](name: String) {
  def ->(value: A): PropertyKeyValue[A] = PropertyKeyValue(this, value)

  def of(value: A): PropertyKeyValue[A] = PropertyKeyValue(this, value)
}

case class PropertyKeyValue[A](key: PropertyKey[A], value: A)
