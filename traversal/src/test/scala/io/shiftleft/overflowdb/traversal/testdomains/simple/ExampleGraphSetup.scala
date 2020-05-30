package io.shiftleft.overflowdb.traversal.testdomains.simple

import io.shiftleft.overflowdb._
import io.shiftleft.overflowdb.traversal.Traversal
import org.apache.tinkerpop.gremlin.structure.{T, Vertex}

/* simple example graph:
 * L3 <- L2 <- L1 <- Center -> R1 -> R2 -> R3 -> R4
 * */
object ExampleGraphSetup {
  val nonExistingLabel = "this label does not exist"
  val nonExistingPropertyKey = PropertyKey[String]("this property key does not exist")

  def simpleDomain: SimpleDomainTraversalSource = SimpleDomain.traversal(graph)
  def centerTrav: Traversal[Thing] = simpleDomain.things.name("Center")
  def centerNode: Thing = centerTrav.head
  def l2Trav: Traversal[Thing] = simpleDomain.things.name("L2")
  def l2Node: Thing = l2Trav.head
  def r2Trav: Traversal[Thing] = simpleDomain.things.name("R2")
  def r2Node: Thing = r2Trav.head

  lazy val graph = {
    val _graph = SimpleDomain.newGraph

    def addThing(name: String) =
      _graph + (Thing.Label, Thing.Properties.Name -> name)

    val center = addThing("Center")
    val l1 = addThing("L1")
    val r1 = addThing("R1")
    val l2 = addThing("L2")
    val r2 = addThing("R2")
    val l3 = addThing("L3")
    val r3 = addThing("R3")
    val r4 = addThing("R4")

    center --- Connection.Label --> l1
    l1 --- Connection.Label --> l2
    l2 --- Connection.Label --> l3
    center --- Connection.Label --> r1
    r1 --- (Connection.Label, Connection.Properties.Distance -> 10) --> r2
    r2 --- (Connection.Label, Connection.Properties.Distance -> 10) --> r3
    r3 --- (Connection.Label, Connection.Properties.Distance -> 13) --> r4
    _graph
  }

}
