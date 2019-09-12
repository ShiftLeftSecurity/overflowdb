package io.shiftleft.overflowdb.traversals.testdomains

import io.shiftleft.overflowdb.traversals.Traversal

package object gratefuldead {

  implicit def traversalToArtistTraversal(trav: Traversal[Artist]): ArtistTraversal =
    new ArtistTraversal(trav)
  
  implicit def traversalToSongTraversal(trav: Traversal[Song]): SongTraversal =
    new SongTraversal(trav)

  // TODO move to overflowdb-traversal
  // TODO can we just inherit Traversal and make it an `implicit class <: AnyVal`?
  implicit def start[A](a: A): Traversal[A] =
    new Traversal[A](Iterator.single(a))
}
