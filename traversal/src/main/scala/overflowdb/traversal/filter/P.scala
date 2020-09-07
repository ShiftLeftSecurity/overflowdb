package overflowdb.traversal.filter

import scala.util.matching.Regex

/** commonly used predicates e.g. for Traversal.has|hasNot|is steps */
object P {
  def eq[A](a: A): A => Boolean =
    a.==

  def neq[A](a: A): A => Boolean =
    a.!=

  def within[A](values: Set[A]): A => Boolean =
    values.contains

  def within[A](values: A*): A => Boolean =
    within(values.to(Set))

  def without[A](values: Set[A]): A => Boolean =
    a => !values.contains(a)

  def without[A](values: A*): A => Boolean =
    without(values.to(Set))

  def matches(regex: String): String => Boolean =
    matches(regex.r)

  def matches(regex: Regex): String => Boolean =
    regex.matches

  /* true if (at least) one of the given regexes matches */
  def matches(regexes: String*): String => Boolean = {
    val regexes0 = regexes.map(_.r)
    value => regexes0.exists(_.matches(value))
  }

}
