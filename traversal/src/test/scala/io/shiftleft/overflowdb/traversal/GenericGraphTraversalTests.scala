package io.shiftleft.overflowdb.traversal

import io.shiftleft.overflowdb.NodeRef
import io.shiftleft.overflowdb.traversal.testdomains.simple.{Connection, SimpleDomain, SimpleDomainTraversalSource, Thing}
import org.apache.tinkerpop.gremlin.structure.{T, Vertex}
import org.scalatest.{Matchers, WordSpec}

import scala.collection.mutable

/** generic graph traversals, i.e. domain independent */
class GenericGraphTraversalTests extends WordSpec with Matchers {

  "step out" in {
    assertNames(centerTrav.out, Set("L1", "R1"))
    assertNames(centerNode.out, Set("L1", "R1"))
    assertNames(centerTrav.out.out, Set("L2", "R2"))
    assertNames(centerNode.out.out, Set("L2", "R2"))
    assertNames(centerTrav.out(Connection.Label), Set("L1", "R1"))
    assertNames(centerNode.out(Connection.Label), Set("L1", "R1"))
    assertNames(centerTrav.out(nonExistingLabel), Set.empty)
    assertNames(centerNode.out(nonExistingLabel), Set.empty)
  }

  "step in" in {
    l2Trav.in.size shouldBe 1
    l2Node.in.size shouldBe 1
    assertNames(l2Trav.in, Set("L1"))
    assertNames(l2Node.in, Set("L1"))
    assertNames(l2Trav.in.in, Set("Center"))
    assertNames(l2Node.in.in, Set("Center"))
    assertNames(l2Trav.in(Connection.Label), Set("L1"))
    assertNames(l2Node.in(Connection.Label), Set("L1"))
    assertNames(l2Trav.in(nonExistingLabel), Set.empty)
    assertNames(l2Node.in(nonExistingLabel), Set.empty)
  }

  "step both" in {
    /* L3 <- L2 <- L1 <- Center -> R1 -> R2 -> R3 -> R4 */
    l2Trav.both.size shouldBe 2
    l2Node.both.size shouldBe 2
    assertNames(l2Trav.both, Set("L1", "L3"))
    assertNames(l2Node.both, Set("L1", "L3"))
    assertNames(r2Trav.both, Set("R1", "R3"))
    assertNames(r2Node.both, Set("R1", "R3"))
    assertNames(l2Trav.both.both, Set("L2", "Center"))
    assertNames(l2Node.both.both, Set("L2", "Center"))
    assertNames(r2Trav.both.both, Set("Center", "R2", "R4"))
    assertNames(r2Node.both.both, Set("Center", "R2", "R4"))
    assertNames(l2Trav.both(Connection.Label), Set("L1", "L3"))
    assertNames(l2Node.both(Connection.Label), Set("L1", "L3"))
    assertNames(l2Trav.both(nonExistingLabel), Set.empty)
    assertNames(l2Node.both(nonExistingLabel), Set.empty)
  }

  "step outE" in {
    centerTrav.outE.size shouldBe 2
    centerNode.outE.size shouldBe 2
    assertNames(centerTrav.outE.inV, Set("L1", "R1"))
    assertNames(centerNode.outE.inV, Set("L1", "R1"))
    assertNames(centerTrav.outE.inV.outE.inV, Set("L2", "R2"))
    assertNames(centerNode.outE.inV.outE.inV, Set("L2", "R2"))
    assertNames(centerTrav.outE(Connection.Label).inV, Set("L1", "R1"))
    assertNames(centerNode.outE(Connection.Label).inV, Set("L1", "R1"))
    assertNames(centerTrav.outE(nonExistingLabel).inV, Set.empty)
    assertNames(centerNode.outE(nonExistingLabel).inV, Set.empty)
  }

  "step inE" in {
    l2Trav.inE.size shouldBe 1
    l2Node.inE.size shouldBe 1
    assertNames(l2Trav.inE.outV, Set("L1"))
    assertNames(l2Node.inE.outV, Set("L1"))
    assertNames(l2Trav.inE.outV.inE.outV, Set("Center"))
    assertNames(l2Node.inE.outV.inE.outV, Set("Center"))
    assertNames(l2Trav.inE(Connection.Label).outV, Set("L1"))
    assertNames(l2Node.inE(Connection.Label).outV, Set("L1"))
    assertNames(l2Trav.inE(nonExistingLabel).outV, Set.empty)
    assertNames(l2Node.inE(nonExistingLabel).outV, Set.empty)
  }

  "step bothE" in {
    /* L3 <- L2 <- L1 <- Center -> R1 -> R2 -> R3 -> R4 */
    l2Trav.bothE.size shouldBe 2
    l2Node.bothE.size shouldBe 2
    l2Trav.bothE(Connection.Label).size shouldBe 2
    l2Node.bothE(Connection.Label).size shouldBe 2
    l2Trav.bothE(nonExistingLabel).size shouldBe 0
    l2Node.bothE(nonExistingLabel).size shouldBe 0
  }

  val nonExistingLabel = "this label does not exist"

  def simpleDomain: SimpleDomainTraversalSource = SimpleDomain.traversal(simpleGraph)
  def centerTrav: Traversal[Thing] = simpleDomain.things.name("Center")
  def centerNode: Thing = centerTrav.head
  def l2Trav: Traversal[Thing] = simpleDomain.things.name("L2")
  def l2Node: Thing = l2Trav.head
  def r2Trav: Traversal[Thing] = simpleDomain.things.name("R2")
  def r2Node: Thing = r2Trav.head

  def assertNames[A <: NodeRef[_]](traversal: Traversal[A], expectedNames: Set[String]) = {
    traversal.property(Thing.Properties.Name).toSet shouldBe expectedNames
  }

  /* L3 <- L2 <- L1 <- Center -> R1 -> R2 -> R3 -> R4 */
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
