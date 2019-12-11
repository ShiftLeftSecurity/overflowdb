package io.shiftleft.overflowdb.traversal

import io.shiftleft.overflowdb.NodeRef
import io.shiftleft.overflowdb.traversal.testdomains.simple.{Connection, SimpleDomain, Thing}
import org.apache.tinkerpop.gremlin.structure.{T, Vertex}
import org.scalatest.{Matchers, WordSpec}

/** generic traversal steps (domain independent) */
class TraversalTests extends WordSpec with Matchers {
  "domain overview" in {
    simpleDomain.all.property(Thing.Properties.Name).toSet shouldBe Set("L3", "L2", "L1", "Center", "R1", "R2", "R3")
    center.head.name shouldBe "Center"
    simpleDomain.all.label.toSet shouldBe Set(Thing.Label)
  }

  "out step" in {
    assertNames(center.out, Set("L1", "R1"))
    assertNames(center.out.out, Set("L2", "R2"))
    assertNames(center.out(Connection.Label), Set("L1", "R1"))
    assertNames(center.out(nonExistingLabel), Set.empty)
  }

  "outE step" in {
    center.outE.size shouldBe 2
    assertNames(center.outE.inV, Set("L1", "R1"))
    assertNames(center.outE.inV.outE.inV, Set("L2", "R2"))
    assertNames(center.outE(Connection.Label).inV, Set("L1", "R1"))
    assertNames(center.outE(nonExistingLabel).inV, Set.empty)
  }

  "can only be iterated once" in {
    val one = Traversal.fromSingle("one")
    one.size shouldBe 1
    one.size shouldBe 0 // logs a warning (not tested here)

    val empty = Traversal(Nil)
    empty.size shouldBe 0
    empty.size shouldBe 0 // logs a warning (not tested here)
  }

  /* L3 <- L2 <- L1 <- Center -> R1 -> R2 -> R3 */
  lazy val simpleGraph = {
    val graph = SimpleDomain.newGraph
    def addThing(name: String): Vertex =
      graph.addVertex(T.label, Thing.Label, Thing.PropertyNames.Name, name)

    val l3 = addThing("L3")
    val l2 = addThing("L2")
    val l1 = addThing("L1")
    val center = addThing("Center")
    val r1 = addThing("R1")
    val r2 = addThing("R2")
    val r3 = addThing("R3")

    center.addEdge(Connection.Label, l1)
    l1.addEdge(Connection.Label, l2)
    l2.addEdge(Connection.Label, l3)
    center.addEdge(Connection.Label, r1)
    r1.addEdge(Connection.Label, r2)
    r2.addEdge(Connection.Label, r3)
    graph
  }

  val nonExistingLabel = "this label does not exist"
  def simpleDomain = SimpleDomain.traversal(simpleGraph)
  def center = simpleDomain.things.name("Center")
  def assertNames[A <: NodeRef[_]](traversal: Traversal[A], expectedNames: Set[String]) = {
    traversal.property(Thing.Properties.Name).toSet shouldBe expectedNames
  }
}
