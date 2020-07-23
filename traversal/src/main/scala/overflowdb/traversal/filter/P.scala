package overflowdb.traversal.filter

/** commonly used predicates e.g. for Traversal.has|hasNot|is steps */
object P {
  def eq[A](a: A): A => Boolean =
    a.==

  def neq[A](a: A): A => Boolean =
    a.!=

  def matches(regex: String): String => Boolean =
    _.matches(regex)

  def within[A](values: Set[A]): A => Boolean =
    values.contains

  def within[A](values: A*): A => Boolean =
    within(values.to(Set))

  def without[A](values: Set[A]): A => Boolean =
    { a: A => !values.contains(a) }

  def without[A](values: A*): A => Boolean =
    without(values.to(Set))

}
