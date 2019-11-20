package io.shiftleft.overflowdb.traversals.testdomains

import io.shiftleft.overflowdb.traversals.Traversal
import io.shiftleft.overflowdb.traversals.filters.StringPropertyFilters

package object gratefuldead {

  implicit class ArtistTraversal(val trav: Traversal[Artist]) extends AnyVal {
    def name: Traversal[String] = trav.map(_.name)

    def nameExact(value: String): Traversal[Artist] = StringPropertyFilters.filterExact(trav)(_.name, value)
    def nameExact(values: String*): Traversal[Artist] = StringPropertyFilters.filterExactMultiple(trav)(_.name, values)

    def nameContains(value: String): Traversal[Artist] = StringPropertyFilters.filterContains(trav)(_.name, value)
    def nameStartsWith(value: String): Traversal[Artist] = StringPropertyFilters.filterStartsWith(trav)(_.name, value)
    def nameEndsWith(value: String): Traversal[Artist] = StringPropertyFilters.filterEndsWith(trav)(_.name, value)

    def name(regexp: String): Traversal[Artist] = StringPropertyFilters.filterRegexp(trav)(_.name, regexp)
    def name(regexps: String*): Traversal[Artist] = StringPropertyFilters.filterRegexpMultiple(trav)(_.name, regexps)

    def nameNot(regexp: String): Traversal[Artist] = StringPropertyFilters.filterNotRegexp(trav)(_.name, regexp)
    def nameNot(regexps: String*): Traversal[Artist] = StringPropertyFilters.filterNotRegexpMultiple(trav)(_.name, regexps)

    def sangSongs: Traversal[Song] = trav.flatMap(_.sangSongs)
  }

  implicit class SongTraversal(val trav: Traversal[Song]) extends AnyVal {
    def name: Traversal[String] = trav.map(_.name)

    def nameExact(value: String): Traversal[Song] = StringPropertyFilters.filterExact(trav)(_.name, value)
    def nameExact(values: String*): Traversal[Song] = StringPropertyFilters.filterExactMultiple(trav)(_.name, values)

    def songType: Traversal[String] = trav.map(_.songType)
    def performances: Traversal[Int] = trav.map(_.performances)
    def followedBy: Traversal[Song] = trav.flatMap(_.followedBy)
  }
}
