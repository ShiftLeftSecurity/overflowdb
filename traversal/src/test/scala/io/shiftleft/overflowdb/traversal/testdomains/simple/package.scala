package io.shiftleft.overflowdb.traversal.testdomains

import io.shiftleft.overflowdb.traversal.Traversal
import io.shiftleft.overflowdb.traversal.filter.{PropertyFilter, StringPropertyFilter}
import io.shiftleft.overflowdb.traversal.help
import io.shiftleft.overflowdb.traversal.help.Doc

package object simple {

  @help.Traversal(elementType = classOf[Thing])
  implicit class ThingTraversal(val trav: Traversal[Thing]) extends AnyVal {
    def followedBy: Traversal[Thing] = trav.flatMap(_.followedBy)

    @Doc("name of the Thing")
    def name: Traversal[String] = trav.map(_.name)

    def name(regexp: String): Traversal[Thing] = StringPropertyFilter.regexp(trav)(_.name, regexp)
    def name(regexps: String*): Traversal[Thing] = StringPropertyFilter.regexpMultiple(trav)(_.name, regexps)
    def nameNot(regexp: String): Traversal[Thing] = StringPropertyFilter.regexpNot(trav)(_.name, regexp)
    def nameNot(regexps: String*): Traversal[Thing] = StringPropertyFilter.regexpNotMultiple(trav)(_.name, regexps)
    def nameExact(value: String): Traversal[Thing] = PropertyFilter.exact(trav)(_.name, value)
    def nameExact(values: String*): Traversal[Thing] = PropertyFilter.exactMultiple(trav)(_.name, values)
    def nameContains(value: String): Traversal[Thing] = StringPropertyFilter.contains(trav)(_.name, value)
    def nameContainsNot(value: String): Traversal[Thing] = StringPropertyFilter.containsNot(trav)(_.name, value)
    def nameStartsWith(value: String): Traversal[Thing] = StringPropertyFilter.startsWith(trav)(_.name, value)
    def nameEndsWith(value: String): Traversal[Thing] = StringPropertyFilter.endsWith(trav)(_.name, value)
  }
}
