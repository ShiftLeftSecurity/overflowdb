package io.shiftleft.overflowdb.traversals.testdomains

import io.shiftleft.overflowdb.traversals.Traversal

package object gratefuldead {

  implicit def traversalToArtistTraversal(trav: Traversal[Artist]): ArtistTraversal =
    new ArtistTraversal(trav)
  
  implicit def traversalToSongTraversal(trav: Traversal[Song]): SongTraversal =
    new SongTraversal(trav)

}
