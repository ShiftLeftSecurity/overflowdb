package overflowdb.traversal

import org.scalatest.{Matchers, WordSpec}
import overflowdb.traversal.testdomains.simple.{ExampleGraphSetup, Thing}

import scala.collection.mutable

class TraversalTests extends WordSpec with Matchers {
  import ExampleGraphSetup._

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
    centerTrav.head.name shouldBe "Center"
    simpleDomain.all.label.toSet shouldBe Set(Thing.Label)
  }

  "repeat" should {
    "be lazily evaluated" in {
      val traversedNodes = mutable.ListBuffer.empty[Thing]
      val traversalNotYetExecuted = {
        centerTrav.repeat(_.sideEffect(traversedNodes.addOne).followedBy)
        centerTrav.repeat(_.sideEffect(traversedNodes.addOne).out)
      }
      withClue("traversal should not do anything when it's only created") {
        traversedNodes.size shouldBe 0
      }
    }

    "by default traverse all nodes to outer limits exactly once, emitting and returning nothing" in {
      val traversedNodes = mutable.ListBuffer.empty[Thing]
      val results = centerTrav.repeat(_.sideEffect(traversedNodes.addOne).out).toList
      traversedNodes.size shouldBe 8
      results.size shouldBe 0
    }

    "emit everything along the way if so configured" in {
      centerTrav.repeat(_.followedBy)(_.emit).name.toSet shouldBe Set("L3", "L2", "L1", "Center", "R1", "R2", "R3", "R4")
      centerTrav.repeat(_.out)(_.emit).property("name").toSet shouldBe Set("L3", "L2", "L1", "Center", "R1", "R2", "R3", "R4")
    }

    "emit nodes that meet given condition" in {
      val results = centerTrav.repeat(_.followedBy)(_.emit(_.name.startsWith("L"))).name.toSet
      results shouldBe Set("L1", "L2", "L3")
    }

    "support arbitrary `until` condition" when {
      "used without emit" in {
        centerTrav.repeat(_.followedBy)(_.until(_.name.endsWith("2"))).name.toSet shouldBe Set("L2", "R2")

        withClue("asserting more fine-grained traversal characteristics") {
          val traversedNodes = mutable.ListBuffer.empty[Thing]
          val traversal = centerTrav.repeat(_.sideEffect(traversedNodes.addOne).followedBy)(_.until(_.name.endsWith("2"))).name

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
        centerTrav.repeat(_.followedBy)(_.until(_.name.endsWith("2")).emit).name.toSet shouldBe Set("Center", "L1", "L2", "R1", "R2")

        import Thing.Properties.Name
        centerTrav.repeat(_.out)(_.until(_.property(Name).endsWith("2")).emit).property(Name).toSet shouldBe Set("Center", "L1", "L2", "R1", "R2")
      }
    }

    "support `times` modulator" when {
      "used without emit" in {
        val results = centerTrav.repeat(_.followedBy)(_.times(2)).name.toSet
        results shouldBe Set("L2", "R2")

        withClue("asserting more fine-grained traversal characteristics") {
          val traversedNodes = mutable.ListBuffer.empty[Thing]
          val traversal = centerTrav.repeat(_.sideEffect(traversedNodes.addOne).followedBy)(_.times(2)).name

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
        val results = centerTrav.repeat(_.followedBy)(_.times(2).emit).name.toSet
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
        thingTraversal.helpVerbose should include(".label") // step from ElementTraversal
        thingTraversal.helpVerbose should include(".out") // step from NodeTraversal
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

}
