package overflowdb.traversal.testdomains

import overflowdb.traversal.{Traversal, help}
import overflowdb.traversal.help.Doc

package object hierarchical {

  trait Animal {
    def species: String
  }

  trait Mammal extends Animal {
    def canSwim: Boolean
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

  @help.Traversal(elementType = classOf[Mammal])
  implicit class MammalTraversal[A <: Mammal](val trav: Traversal[A]) extends AnyVal {
    @Doc("can this mammal swim?")
    def canSwim: Traversal[Boolean] = trav.map(_.canSwim)
  }

  @help.Traversal(elementType = classOf[Elephant])
  implicit class ElephantTraversal(val trav: Traversal[Elephant]) extends AnyVal {
    @Doc("name of the elephant")
    def name: Traversal[String] = trav.map(_.name)
  }

}
