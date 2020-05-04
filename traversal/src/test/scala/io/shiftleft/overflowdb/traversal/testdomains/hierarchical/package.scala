package io.shiftleft.overflowdb.traversal.testdomains

import io.shiftleft.overflowdb.traversal.{Traversal, help}
import io.shiftleft.overflowdb.traversal.help.Doc

package object hierarchical {

  trait Animal {
    def species: String
  }

  @help.Traversal(elementType = classOf[Car])
  implicit class CarTraversal(val trav: Traversal[Car]) extends AnyVal {
    @Doc("name of the car")
    def name: Traversal[String] = trav.map(_.name)
  }

  @help.Traversal(elementType = classOf[Animal])
  implicit class AnimalTraversal[A <: Animal](val trav: Traversal[A]) extends AnyVal {
    @Doc("species of the animal")
    def species: Traversal[String] = trav.map(_.species)
  }

  @help.Traversal(elementType = classOf[Elephant])
  implicit class ElephantTraversal(val trav: Traversal[Elephant]) extends AnyVal {
    @Doc("name of the elephant")
    def name: Traversal[String] = trav.map(_.name)
  }

}
