package io.shiftleft.overflowdb.traversal

import io.shiftleft.overflowdb.NodeRef
import io.shiftleft.overflowdb.traversal.testdomains.simple.{Connection, SimpleDomain, SimpleDomainTraversalSource, Thing}
import org.apache.tinkerpop.gremlin.structure.{T, Vertex}
import org.scalatest.{Matchers, WordSpec}

import scala.collection.mutable

/** generic traversal steps (mostly domain independent) */
class TraversalTests extends WordSpec with Matchers {

  "can only be iterated once" in {
    val one = Traversal.fromSingle("one")
    one.size shouldBe 1
    one.size shouldBe 0 // logs a warning (not tested here)

    val empty = Traversal(Nil)
    empty.size shouldBe 0
    empty.size shouldBe 0 // logs a warning (not tested here)
  }

  "perform sideEffect" should {
    def traversal = 1.to(10).to(Traversal)

    "support normal function" in {
      val sack = mutable.ListBuffer.empty[Int]
      traversal.sideEffect(sack.addOne).iterate
      sack.size shouldBe 10
    }

    "support PartialFunction and not fail for undefined cases" in {
      val sack = mutable.ListBuffer.empty[Int]
      traversal.sideEffectPF {
        case i if i > 5 => sack.addOne(i)
      }.iterate
      sack.size shouldBe 5
    }
  }

  "domain overview" in {
    simpleDomain.all.property(Thing.Properties.Name).toSet shouldBe Set("L3", "L2", "L1", "Center", "R1", "R2", "R3", "R4")
    center.head.name shouldBe "Center"
    simpleDomain.all.label.toSet shouldBe Set(Thing.Label)
  }

  "generic graph steps" can {
    "step out" in {
      assertNames(center.out, Set("L1", "R1"))
      assertNames(center.out.out, Set("L2", "R2"))
      assertNames(center.out(Connection.Label), Set("L1", "R1"))
      assertNames(center.out(nonExistingLabel), Set.empty)
    }

    "step in" in {
      l2.in.size shouldBe 1
      assertNames(l2.in, Set("L1"))
      assertNames(l2.in.in, Set("Center"))
      assertNames(l2.in(Connection.Label), Set("L1"))
      assertNames(l2.in(nonExistingLabel), Set.empty)
    }
    
    "step both" in {
      /* L3 <- L2 <- L1 <- Center -> R1 -> R2 -> R3 -> R4 */
      l2.both.size shouldBe 2
      assertNames(l2.both, Set("L1", "L3"))
      assertNames(r2.both, Set("R1", "R3"))
      assertNames(l2.both.both, Set("L2", "Center"))
      assertNames(r2.both.both, Set("Center", "R2", "R4"))
      assertNames(l2.both(Connection.Label), Set("L1", "L3"))
      assertNames(l2.both(nonExistingLabel), Set.empty)
    }

    "step outE" in {
      center.outE.size shouldBe 2
      assertNames(center.outE.inV, Set("L1", "R1"))
      assertNames(center.outE.inV.outE.inV, Set("L2", "R2"))
      assertNames(center.outE(Connection.Label).inV, Set("L1", "R1"))
      assertNames(center.outE(nonExistingLabel).inV, Set.empty)
    }

    "step inE" in {
      l2.inE.size shouldBe 1
      assertNames(l2.inE.outV, Set("L1"))
      assertNames(l2.inE.outV.inE.outV, Set("Center"))
      assertNames(l2.inE(Connection.Label).outV, Set("L1"))
      assertNames(l2.inE(nonExistingLabel).outV, Set.empty)
    }

    "step bothE" in {
      /* L3 <- L2 <- L1 <- Center -> R1 -> R2 -> R3 -> R4 */
      l2.bothE.size shouldBe 2
      l2.bothE(Connection.Label).size shouldBe 2
      l2.bothE(nonExistingLabel).size shouldBe 0
    }
  }

  "repeat" should {
    "be lazily evaluated" in {
      val traversedNodes = mutable.ListBuffer.empty[Thing]
      val traversalNotYetExecuted = center.repeat(_.sideEffect(traversedNodes.addOne).followedBy)
      withClue("traversal should not do anything when it's only created") {
        traversedNodes.size shouldBe 0
      }
    }

    "by default traverse all nodes to outer limits exactly once, emitting and returning nothing" in {
      val traversedNodes = mutable.ListBuffer.empty[Thing]
      val results = center.repeat(_.sideEffect(traversedNodes.addOne).followedBy).toList
      traversedNodes.size shouldBe 8
      results.size shouldBe 0
    }

    "emit everything along the way if so configured" in {
      center.repeat(_.followedBy, _.emit).name.toSet shouldBe Set("L3", "L2", "L1", "Center", "R1", "R2", "R3", "R4")
    }

    "emit nodes that meet given condition" in {
      val results = center.repeat(_.followedBy, _.emit(_.name.startsWith("L"))).name.toSet
      results shouldBe Set("L1", "L2", "L3")
    }

    "support arbitrary `until` condition" when {
      "used without emit" in {
        center.repeat(_.followedBy, _.until(_.name.endsWith("2"))).name.toSet shouldBe Set("L2", "R2")

        withClue("asserting more fine-grained traversal characteristics") {
          val traversedNodes = mutable.ListBuffer.empty[Thing]
          val traversal = center.repeat(_.sideEffect(traversedNodes.addOne).followedBy, _.until(_.name.endsWith("2"))).name

          // hasNext will run the provided repeat traversal exactly 2 times (as configured)
          traversal.hasNext shouldBe true
          traversedNodes.size shouldBe 2
          // hasNext is idempotent
          traversal.hasNext shouldBe true
          traversedNodes.size shouldBe 2

          traversal.next shouldBe "L2"
          traversal.next shouldBe "R2"
          traversedNodes.size shouldBe 3
          traversedNodes.map(_.name).to(Set) shouldBe Set("Center", "L1", "R1")
          traversal.hasNext shouldBe false
        }
      }

      "used in combination with emit" in {
        center.repeat(_.followedBy, _.until(_.name.endsWith("2")).emit).name.toSet shouldBe Set("Center", "L1", "L2", "R1", "R2")
      }
    }

    "support `times` modulator" when {
      "used without emit" in {
        val results = center.repeat(_.followedBy, _.times(2)).name.toSet
        results shouldBe Set("L2", "R2")

        withClue("asserting more fine-grained traversal characteristics") {
          val traversedNodes = mutable.ListBuffer.empty[Thing]
          val traversal = center.repeat(_.sideEffect(traversedNodes.addOne).followedBy, _.times(2)).name

          // hasNext will run the provided repeat traversal exactly 2 times (as configured)
          traversal.hasNext shouldBe true
          traversedNodes.size shouldBe 2
          // hasNext is idempotent
          traversal.hasNext shouldBe true
          traversedNodes.size shouldBe 2

          traversal.next shouldBe "L2"
          traversal.next shouldBe "R2"
          traversedNodes.size shouldBe 3
          traversedNodes.map(_.name).to(Set) shouldBe Set("Center", "L1", "R1")
          traversal.hasNext shouldBe false
        }
      }

      "used in combination with emit" in {
        val results = center.repeat(_.followedBy, _.times(2).emit).name.toSet
        results shouldBe Set("Center", "L1", "L2", "R1", "R2")
      }
    }
  }

  ".help step" should {

    "give a domain overview" in {
      simpleDomain.help should include(".things")
      simpleDomain.help should include("all things")
    }

    "provide node-specific overview" when {
      "using simple domain" in {
        val thingTraversal: Traversal[testdomains.simple.Thing] = Traversal.empty
        thingTraversal.help should include("Available steps for Thing")
        thingTraversal.help should include(".name")

        thingTraversal.helpVerbose should include("ThingTraversal") // the Traversal classname
        thingTraversal.helpVerbose should include(".sideEffect") // step from Traversal
        thingTraversal.helpVerbose should include(".label") // step from NodeTraversal
      }

      "using hierarchical domain" in {
        import testdomains.hierarchical.{Animal, Car, Elephant, Mammal}
        Traversal.empty[Animal].help should include("species of the animal")
        Traversal.empty[Mammal].help should include("can this mammal swim?")
        Traversal.empty[Elephant].help should include("name of the elephant")
        Traversal.empty[Car].help should include("name of the car")

        // elephant is a mammal (and therefor an animal)
        Traversal.empty[Elephant].canSwim // only verify that it compiles
        Traversal.empty[Elephant].species // only verify that it compiles
        Traversal.empty[Elephant].help should include("species of the animal")
        Traversal.empty[Elephant].help should include("can this mammal swim?")
      }
    }

    "provides generic help" when {
      "using verbose mode" when {
        "traversing nodes" in {
          val thingTraversal: Traversal[Thing] = Traversal.empty
          thingTraversal.helpVerbose should include(".sideEffect")
          thingTraversal.helpVerbose should include(".label")
        }

        "traversing non-nodes" in {
          val stringTraversal = Traversal.empty[String]
          stringTraversal.helpVerbose should include(".sideEffect")
          stringTraversal.helpVerbose should not include ".label"
        }
      }
    }
  }

  val nonExistingLabel = "this label does not exist"

  def simpleDomain: SimpleDomainTraversalSource = SimpleDomain.traversal(simpleGraph)
  def center: Traversal[Thing] = simpleDomain.things.name("Center")
  def l2: Traversal[Thing] = simpleDomain.things.name("L2")
  def r2: Traversal[Thing] = simpleDomain.things.name("R2")

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
