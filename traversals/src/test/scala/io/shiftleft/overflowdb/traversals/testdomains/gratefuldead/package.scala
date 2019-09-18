package io.shiftleft.overflowdb.traversals.testdomains

import io.shiftleft.overflowdb.traversals.Traversal

package object gratefuldead {

  implicit class ArtistTraversal(val trav: Traversal[Artist]) extends AnyVal {
    def name: Traversal[String] = trav.map(_.name)
    def name(value: String): Traversal[Artist] = trav.filter(_.name == value)

    def sangSongs: Traversal[Song] = trav.flatMap(_.sangSongs)
  }

  implicit class SongTraversal(val trav: Traversal[Song]) extends AnyVal {
    def name: Traversal[String] = trav.map(_.name)
    def name(value: String): Traversal[Song] = trav.filter(_.name == value)

    def songType: Traversal[String] = trav.map(_.songType)
    def performances: Traversal[Int] = trav.map(_.performances)

    def followedBy: Traversal[Song] = trav.flatMap(_.followedBy)
  }

  // TODO move to overflowdb-traversal
  // TODO can we just inherit Traversal and make it an `implicit class <: AnyVal`?
  implicit def start[A](a: A): Traversal[A] =
    new Traversal[A](Iterator.single(a))
}
