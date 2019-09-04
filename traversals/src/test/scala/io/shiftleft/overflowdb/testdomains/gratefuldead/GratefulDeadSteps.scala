package io.shiftleft.overflowdb.testdomains.gratefuldead

import io.shiftleft.overflowdb.OdbGraph
import io.shiftleft.overflowdb.traversals.{Traversal, TraversalSource}

class GratefulDeadTraversalSource(graph: OdbGraph) extends TraversalSource(graph) {
  def artists: Traversal[Artist] = nodesByLabelTyped(Artist.label)
  def songs: Traversal[Song] = nodesByLabelTyped(Song.label)
}

class ArtistTraversal(trav: Traversal[Artist]) extends Traversal[Artist](trav) {
  def name: Traversal[String] = trav.map(_.name())
}