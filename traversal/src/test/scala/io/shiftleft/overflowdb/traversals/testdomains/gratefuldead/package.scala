package io.shiftleft.overflowdb.traversals.testdomains

import io.shiftleft.overflowdb.traversals.Traversal
import io.shiftleft.overflowdb.traversals.filters.{NumberPropertyFilter, PropertyFilter, StringPropertyFilter}

package object gratefuldead {

  implicit class ArtistTraversal(val trav: Traversal[Artist]) extends AnyVal {
    def sangSongs: Traversal[Song] = trav.flatMap(_.sangSongs)

    def name: Traversal[String] = trav.map(_.name)
    def name(regexp: String): Traversal[Artist] = StringPropertyFilter.regexp(trav)(_.name, regexp)
    def name(regexps: String*): Traversal[Artist] = StringPropertyFilter.regexpMultiple(trav)(_.name, regexps)
    def nameNot(regexp: String): Traversal[Artist] = StringPropertyFilter.regexpNot(trav)(_.name, regexp)
    def nameNot(regexps: String*): Traversal[Artist] = StringPropertyFilter.regexpNotMultiple(trav)(_.name, regexps)
    def nameExact(value: String): Traversal[Artist] = PropertyFilter.exact(trav)(_.name, value)
    def nameExact(values: String*): Traversal[Artist] = PropertyFilter.exactMultiple(trav)(_.name, values)
    def nameContains(value: String): Traversal[Artist] = StringPropertyFilter.contains(trav)(_.name, value)
    def nameContainsNot(value: String): Traversal[Artist] = StringPropertyFilter.containsNot(trav)(_.name, value)
    def nameStartsWith(value: String): Traversal[Artist] = StringPropertyFilter.startsWith(trav)(_.name, value)
    def nameEndsWith(value: String): Traversal[Artist] = StringPropertyFilter.endsWith(trav)(_.name, value)
  }

  implicit class SongTraversal(val trav: Traversal[Song]) extends AnyVal {
    def followedBy: Traversal[Song] = trav.flatMap(_.followedBy)
    
    def name: Traversal[String] = trav.map(_.name)
    def name(regexp: String): Traversal[Song] = StringPropertyFilter.regexp(trav)(_.name, regexp)
    def name(regexps: String*): Traversal[Song] = StringPropertyFilter.regexpMultiple(trav)(_.name, regexps)
    def nameNot(regexp: String): Traversal[Song] = StringPropertyFilter.regexpNot(trav)(_.name, regexp)
    def nameNot(regexps: String*): Traversal[Song] = StringPropertyFilter.regexpNotMultiple(trav)(_.name, regexps)
    def nameExact(value: String): Traversal[Song] = PropertyFilter.exact(trav)(_.name, value)
    def nameExact(values: String*): Traversal[Song] = PropertyFilter.exactMultiple(trav)(_.name, values)
    def nameContains(value: String): Traversal[Song] = StringPropertyFilter.contains(trav)(_.name, value)
    def nameContainsNot(value: String): Traversal[Song] = StringPropertyFilter.containsNot(trav)(_.name, value)
    def nameStartsWith(value: String): Traversal[Song] = StringPropertyFilter.startsWith(trav)(_.name, value)
    def nameEndsWith(value: String): Traversal[Song] = StringPropertyFilter.endsWith(trav)(_.name, value)

    def songType: Traversal[String] = trav.map(_.songType)
    def songType(regexp: String): Traversal[Song] = StringPropertyFilter.regexp(trav)(_.songType, regexp)
    def songType(regexps: String*): Traversal[Song] = StringPropertyFilter.regexpMultiple(trav)(_.songType, regexps)
    def songTypeNot(regexp: String): Traversal[Song] = StringPropertyFilter.regexpNot(trav)(_.songType, regexp)
    def songTypeNot(regexps: String*): Traversal[Song] = StringPropertyFilter.regexpNotMultiple(trav)(_.songType, regexps)
    def songTypeExact(value: String): Traversal[Song] = PropertyFilter.exact(trav)(_.songType, value)
    def songTypeExact(values: String*): Traversal[Song] = PropertyFilter.exactMultiple(trav)(_.songType, values)
    def songTypeContains(value: String): Traversal[Song] = StringPropertyFilter.contains(trav)(_.songType, value)
    def songTypeContainsNot(value: String): Traversal[Song] = StringPropertyFilter.containsNot(trav)(_.songType, value)
    def songTypeStartsWith(value: String): Traversal[Song] = StringPropertyFilter.startsWith(trav)(_.songType, value)
    def songTypeEndsWith(value: String): Traversal[Song] = StringPropertyFilter.endsWith(trav)(_.songType, value)

    def performances: Traversal[Int] = trav.map(_.performances)
    def performances(value: Int): Traversal[Song] = PropertyFilter.exact(trav)(_.performances, value)
    def performancesGt(value: Int): Traversal[Song] = NumberPropertyFilter.Int.gt(trav)(_.performances, value)
    def performancesGte(value: Int): Traversal[Song] = NumberPropertyFilter.Int.gte(trav)(_.performances, value)
    def performancesLt(value: Int): Traversal[Song] = NumberPropertyFilter.Int.lt(trav)(_.performances, value)
    def performancesLte(value: Int): Traversal[Song] = NumberPropertyFilter.Int.lte(trav)(_.performances, value)
  }
}
