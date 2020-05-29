package io.shiftleft.overflowdb.traversal.testdomains.simple

import io.shiftleft.overflowdb.traversal.Traversal
import org.apache.tinkerpop.gremlin.structure.{T, Vertex}

/* simple example graph:
 * L3 <- L2 <- L1 <- Center -> R1 -> R2 -> R3 -> R4
 * */
object ExampleGraphSetup {
  val nonExistingLabel = "this label does not exist"

  def simpleDomain: SimpleDomainTraversalSource = SimpleDomain.traversal(simpleGraph)
  def centerTrav: Traversal[Thing] = simpleDomain.things.name("Center")
  def centerNode: Thing = centerTrav.head
  def l2Trav: Traversal[Thing] = simpleDomain.things.name("L2")
  def l2Node: Thing = l2Trav.head
  def r2Trav: Traversal[Thing] = simpleDomain.things.name("R2")
  def r2Node: Thing = r2Trav.head

  lazy val simpleGraph = {
    val graph = SimpleDomain.newGraph

    def addThing(name: String): Vertex =
      graph.addVertex(T.label, Thing.Label, Thing.PropertyNames.Name, name)

    val center = addThing("Center")
    val l1 = addThing("L1")
    val r1 = addThing("R1")
    val l2 = addThing("L2")
    val r2 = addThing("R2")
    val l3 = addThing("L3")
    val r3 = addThing("R3")
    val r4 = addThing("R4")

    center.addEdge(Connection.Label, l1)
    l1.addEdge(Connection.Label, l2)
    l2.addEdge(Connection.Label, l3)
    center.addEdge(Connection.Label, r1)
    r1.addEdge(Connection.Label, r2)
    r2.addEdge(Connection.Label, r3)
    r3.addEdge(Connection.Label, r4)
    graph
  }

}
