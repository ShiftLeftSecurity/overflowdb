package io.shiftleft.overflowdb.traversals.testdomains

import io.shiftleft.overflowdb.traversals.Traversal
import io.shiftleft.overflowdb.traversals.filters.{NumberPropertyFilters, PropertyFilters, StringPropertyFilters}

package object gratefuldead {

  implicit class ArtistTraversal(val trav: Traversal[Artist]) extends AnyVal {
    def sangSongs: Traversal[Song] = trav.flatMap(_.sangSongs)

    def name: Traversal[String] = trav.map(_.name)
    def name(regexp: String): Traversal[Artist] = StringPropertyFilters.regexp(trav)(_.name, regexp)
    def name(regexps: String*): Traversal[Artist] = StringPropertyFilters.regexpMultiple(trav)(_.name, regexps)
    def nameNot(regexp: String): Traversal[Artist] = StringPropertyFilters.regexpNot(trav)(_.name, regexp)
    def nameNot(regexps: String*): Traversal[Artist] = StringPropertyFilters.regexpNotMultiple(trav)(_.name, regexps)
    def nameExact(value: String): Traversal[Artist] = PropertyFilters.exact(trav)(_.name, value)
    def nameExact(values: String*): Traversal[Artist] = PropertyFilters.exactMultiple(trav)(_.name, values)
    def nameContains(value: String): Traversal[Artist] = StringPropertyFilters.contains(trav)(_.name, value)
    def nameContainsNot(value: String): Traversal[Artist] = StringPropertyFilters.containsNot(trav)(_.name, value)
    def nameStartsWith(value: String): Traversal[Artist] = StringPropertyFilters.startsWith(trav)(_.name, value)
    def nameEndsWith(value: String): Traversal[Artist] = StringPropertyFilters.endsWith(trav)(_.name, value)
  }

  implicit class SongTraversal(val trav: Traversal[Song]) extends AnyVal {
    def followedBy: Traversal[Song] = trav.flatMap(_.followedBy)
    
    def name: Traversal[String] = trav.map(_.name)
    def name(regexp: String): Traversal[Song] = StringPropertyFilters.regexp(trav)(_.name, regexp)
    def name(regexps: String*): Traversal[Song] = StringPropertyFilters.regexpMultiple(trav)(_.name, regexps)
    def nameNot(regexp: String): Traversal[Song] = StringPropertyFilters.regexpNot(trav)(_.name, regexp)
    def nameNot(regexps: String*): Traversal[Song] = StringPropertyFilters.regexpNotMultiple(trav)(_.name, regexps)
    def nameExact(value: String): Traversal[Song] = PropertyFilters.exact(trav)(_.name, value)
    def nameExact(values: String*): Traversal[Song] = PropertyFilters.exactMultiple(trav)(_.name, values)
    def nameContains(value: String): Traversal[Song] = StringPropertyFilters.contains(trav)(_.name, value)
    def nameContainsNot(value: String): Traversal[Song] = StringPropertyFilters.containsNot(trav)(_.name, value)
    def nameStartsWith(value: String): Traversal[Song] = StringPropertyFilters.startsWith(trav)(_.name, value)
    def nameEndsWith(value: String): Traversal[Song] = StringPropertyFilters.endsWith(trav)(_.name, value)

    def songType: Traversal[String] = trav.map(_.songType)
    def songType(regexp: String): Traversal[Song] = StringPropertyFilters.regexp(trav)(_.songType, regexp)
    def songType(regexps: String*): Traversal[Song] = StringPropertyFilters.regexpMultiple(trav)(_.songType, regexps)
    def songTypeNot(regexp: String): Traversal[Song] = StringPropertyFilters.regexpNot(trav)(_.songType, regexp)
    def songTypeNot(regexps: String*): Traversal[Song] = StringPropertyFilters.regexpNotMultiple(trav)(_.songType, regexps)
    def songTypeExact(value: String): Traversal[Song] = PropertyFilters.exact(trav)(_.songType, value)
    def songTypeExact(values: String*): Traversal[Song] = PropertyFilters.exactMultiple(trav)(_.songType, values)
    def songTypeContains(value: String): Traversal[Song] = StringPropertyFilters.contains(trav)(_.songType, value)
    def songTypeContainsNot(value: String): Traversal[Song] = StringPropertyFilters.containsNot(trav)(_.songType, value)
    def songTypeStartsWith(value: String): Traversal[Song] = StringPropertyFilters.startsWith(trav)(_.songType, value)
    def songTypeEndsWith(value: String): Traversal[Song] = StringPropertyFilters.endsWith(trav)(_.songType, value)

    def performances: Traversal[Int] = trav.map(_.performances)
    def performances(value: Int): Traversal[Song] = PropertyFilters.exact(trav)(_.performances, value)
    def performancesGt(value: Int): Traversal[Song] = NumberPropertyFilters.Int.gt(trav)(_.performances, value)
    def performancesGte(value: Int): Traversal[Song] = NumberPropertyFilters.Int.gte(trav)(_.performances, value)
    def performancesLt(value: Int): Traversal[Song] = NumberPropertyFilters.Int.lt(trav)(_.performances, value)
    def performancesLte(value: Int): Traversal[Song] = NumberPropertyFilters.Int.lte(trav)(_.performances, value)
  }
}
