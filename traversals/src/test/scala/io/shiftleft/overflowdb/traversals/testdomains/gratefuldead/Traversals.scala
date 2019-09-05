package io.shiftleft.overflowdb.traversals.testdomains.gratefuldead

import io.shiftleft.overflowdb.OdbGraph
import io.shiftleft.overflowdb.traversals.{Traversal, TraversalSource}

class GratefulDeadTraversalSource(graph: OdbGraph) extends TraversalSource(graph) {
  def artists: Traversal[Artist] = nodesByLabelTyped(Artist.Label)
  def songs: Traversal[Song] = nodesByLabelTyped(Song.Label)
}

class ArtistTraversal(trav: Traversal[Artist]) extends Traversal[Artist](trav) {
  def name: Traversal[String] = trav.map(_.name)
  def name(value: String): Traversal[Artist] = trav.filter(_.name == value)

  def sangSongs: Traversal[Song] = trav.flatMap(_.sangSongs)
}

class SongTraversal(trav: Traversal[Song]) extends Traversal[Song](trav) {
  def name: Traversal[String] = trav.map(_.name)
  def name(value: String): Traversal[Song] = trav.filter(_.name == value)

  def songType: Traversal[String] = trav.map(_.songType)
  def performances: Traversal[Int] = trav.map(_.performances)

  def followedBy: Traversal[Song] = trav.flatMap(_.followedBy)
}
