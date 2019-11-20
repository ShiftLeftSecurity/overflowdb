package io.shiftleft.overflowdb.traversals.testdomains

import io.shiftleft.overflowdb.traversals.Traversal
import io.shiftleft.overflowdb.traversals.filters.{PropertyFilters, StringPropertyFilters}

package object gratefuldead {

  implicit class ArtistTraversal(val trav: Traversal[Artist]) extends AnyVal {
    def name: Traversal[String] = trav.map(_.name)

    def nameExact(value: String): Traversal[Artist] = PropertyFilters.exact(trav)(_.name, value)
    def nameExact(values: String*): Traversal[Artist] = PropertyFilters.exactMultiple(trav)(_.name, values)

    def nameContains(value: String): Traversal[Artist] = StringPropertyFilters.contains(trav)(_.name, value)
    def nameContainsNot(value: String): Traversal[Artist] = StringPropertyFilters.containsNot(trav)(_.name, value)
    def nameStartsWith(value: String): Traversal[Artist] = StringPropertyFilters.startsWith(trav)(_.name, value)
    def nameEndsWith(value: String): Traversal[Artist] = StringPropertyFilters.endsWith(trav)(_.name, value)

    def name(regexp: String): Traversal[Artist] = StringPropertyFilters.regexp(trav)(_.name, regexp)
    def name(regexps: String*): Traversal[Artist] = StringPropertyFilters.regexpMultiple(trav)(_.name, regexps)

    def nameNot(regexp: String): Traversal[Artist] = StringPropertyFilters.regexpNot(trav)(_.name, regexp)
    def nameNot(regexps: String*): Traversal[Artist] = StringPropertyFilters.regexpNotMultiple(trav)(_.name, regexps)

    def sangSongs: Traversal[Song] = trav.flatMap(_.sangSongs)
  }

  implicit class SongTraversal(val trav: Traversal[Song]) extends AnyVal {
    def name: Traversal[String] = trav.map(_.name)

    def nameExact(value: String): Traversal[Song] = PropertyFilters.exact(trav)(_.name, value)
    def nameExact(values: String*): Traversal[Song] = PropertyFilters.exactMultiple(trav)(_.name, values)

    def songType: Traversal[String] = trav.map(_.songType)
    def performances: Traversal[Int] = trav.map(_.performances)
    def followedBy: Traversal[Song] = trav.flatMap(_.followedBy)
  }
}
