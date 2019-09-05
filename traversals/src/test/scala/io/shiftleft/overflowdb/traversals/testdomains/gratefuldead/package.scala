package io.shiftleft.overflowdb.traversals.testdomains

import io.shiftleft.overflowdb.traversals.Traversal

package object gratefuldead {

  implicit def toArtistTraversal(trav: Traversal[Artist]): ArtistTraversal =
    new ArtistTraversal(trav)
  
  implicit def toSongTraversal(trav: Traversal[Song]): SongTraversal =
    new SongTraversal(trav)

}
