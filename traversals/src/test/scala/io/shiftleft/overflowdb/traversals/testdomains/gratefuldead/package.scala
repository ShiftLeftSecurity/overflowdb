package io.shiftleft.overflowdb.traversals.testdomains

import io.shiftleft.overflowdb.traversals.Traversal

package object gratefuldead {

  implicit class ArtistTraversal(val trav: Traversal[Artist]) extends AnyVal {
    def name: Traversal[String] = trav.map(_.name)

    def nameExact(value: String): Traversal[Artist] =
      trav.filter(_.name == value)
    def nameExact(values: String*): Traversal[Artist] = {
      val valuesSet: Set[String] = values.to(Set)
      trav.filter(artist => valuesSet.contains(artist.name))
    }
    def name(regexp: String): Traversal[Artist] = {
      val valueRegexp = regexp.r
      trav.filter { node => valueRegexp.matches(node.name) }
    }
    def name(regexps: String*): Traversal[Artist] = {
      val valueRegexps = regexps.map(_.r)
      trav.filter { node => valueRegexps.find(_.matches(node.name)).isDefined }
    }
    def nameNot(regexp: String): Traversal[Artist] = {
      val valueRegexp = regexp.r
      trav.filter { node => !valueRegexp.matches(node.name) }
    }
    def nameNot(regexps: String*): Traversal[Artist] = {
      val valueRegexps = regexps.map(_.r)
      trav.filter { node => valueRegexps.find(_.matches(node.name)).isEmpty }
    }

    def sangSongs: Traversal[Song] = trav.flatMap(_.sangSongs)
  }

  implicit class SongTraversal(val trav: Traversal[Song]) extends AnyVal {
    def name: Traversal[String] = trav.map(_.name)
    def nameExact(value: String): Traversal[Song] = trav.filter(_.name == value)
    def nameExact(values: String*): Traversal[Song] = {
      val valuesSet: Set[String] = values.to(Set)
      trav.filter(song => valuesSet.contains(song.name))
    }

    def songType: Traversal[String] = trav.map(_.songType)
    def performances: Traversal[Int] = trav.map(_.performances)

    def followedBy: Traversal[Song] = trav.flatMap(_.followedBy)
  }
}
