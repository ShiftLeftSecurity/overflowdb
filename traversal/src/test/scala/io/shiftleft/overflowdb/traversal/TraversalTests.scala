package io.shiftleft.overflowdb.traversal

import io.shiftleft.overflowdb.NodeRef
import io.shiftleft.overflowdb.traversal.testdomains.simple.{Connection, SimpleDomain, SimpleDomainTraversalSource, Thing}
import org.apache.tinkerpop.gremlin.structure.{T, Vertex}
import org.scalatest.{Matchers, WordSpec}

import scala.collection.mutable

/** generic traversal steps (mostly domain independent) */
class TraversalTests extends WordSpec with Matchers {
  "domain overview" in {
    simpleDomain.all.property(Thing.Properties.Name).toSet shouldBe Set("L3", "L2", "L1", "Center", "R1", "R2", "R3", "R4")
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

  "repeat" should {
    "traverse all nodes to outer limits exactly once, emitting nothing" in {
      val traversedNodes = mutable.ListBuffer.empty[NodeRef[_]]
      val results = center.repeat(_.sideEffect(traversedNodes.addOne).followedBy).l
      traversedNodes.size shouldBe 8
      results.size shouldBe 0
    }

    "be lazily evaluated" in {
      val traversedNodes = mutable.ListBuffer.empty[NodeRef[_]]
//      val repeatTraversal = center.repeat(_.sideEffect(traversedNodes.addOne).followedBy)
      val repeatTraversal = center.repeat(_.followedBy)
      traversedNodes.size shouldBe 0
      repeatTraversal.next.name shouldBe "Center"
      traversedNodes.size shouldBe 1
      repeatTraversal.next
      traversedNodes.size shouldBe 2
    }

    "emit everything along the way if so configured" in {
      val results = center.repeat(_.followedBy, _.emit).l
      results.size shouldBe 8
    }

    "emit nodes that meet given condition" in {
      val results = center.repeat(_.followedBy, _.emit(_.name.startsWith("L"))).name.toSet
      results shouldBe Set("L1", "L2", "L3")
    }

    "allow arbitrary `until` condition" in {
      val results = center.repeat(_.followedBy, _.until(_.name.endsWith("2")).emit).name.toSet
      results shouldBe Set("Center", "L1", "L2", "R1", "R2")
    }

    "allow `maxDepth` modulator" in {
      val results = center.repeat(_.followedBy, _.maxDepth(2).emit).name.toSet
      results shouldBe Set("Center", "L1", "L2", "R1", "R2")
    }


    "foo" in {
//      center.repeat(_.sideEffect(t => println(t.name)).followedBy).toList
//      println(center.repeat(_.sideEffect(t => println(t.name)).followedBy, _.emit).take(2))
      val iter = center.repeat(_.sideEffect(t => println("in repeat: " + t.name)).followedBy, _.emit)
      println(iter)
      println(iter.head)
      println(iter.head)
//      println(iter.next)
    }
  }

  "Traversal can only be iterated once" in {
    val one = Traversal.fromSingle("one")
    one.size shouldBe 1
    one.size shouldBe 0 // logs a warning (not tested here)

    val empty = Traversal(Nil)
    empty.size shouldBe 0
    empty.size shouldBe 0 // logs a warning (not tested here)
  }

  val nonExistingLabel = "this label does not exist"

  def simpleDomain: SimpleDomainTraversalSource = SimpleDomain.traversal(simpleGraph)
  def center: Traversal[Thing] = simpleDomain.things.name("Center")

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
