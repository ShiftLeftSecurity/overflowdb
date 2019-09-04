package io.shiftleft.overflowdb.testdomains

import io.shiftleft.overflowdb.traversals.Traversal

package object gratefuldead {

  implicit def toArtistTraversal(trav: Traversal[Artist]): ArtistTraversal =
    new ArtistTraversal(trav)

}
