package io.shiftleft.overflowdb.traversal.testdomains

import io.shiftleft.overflowdb.traversal.Traversal
import io.shiftleft.overflowdb.traversal.filter.{PropertyFilter, StringPropertyFilter}
import io.shiftleft.overflowdb.traversal.help
import io.shiftleft.overflowdb.traversal.help.Doc

package object hierarchical {

  @help.Traversal(elementType = classOf[Elephant])
  implicit class ElephantTraversal(val trav: Traversal[Elephant]) extends AnyVal {
    def followedBy: Traversal[Elephant] = trav.flatMap(_.followedBy)

    @Doc("name of the elephant")
    def name: Traversal[String] = trav.map(_ => "jumbo")
  }
}
