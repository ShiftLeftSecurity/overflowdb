package overflowdb.traversal

import org.scalatest.{Matchers, WordSpec}
import overflowdb.traversal.testdomains.simple.{ExampleGraphSetup, Thing}

import scala.collection.mutable


class RepeatTraversalTests extends WordSpec with Matchers {
  import ExampleGraphSetup._

  "be lazy" in {
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
    val results =
      centerTrav.repeat(_.sideEffect(traversedNodes.addOne).out).l ++
      centerTrav.repeat(_.sideEffect(traversedNodes.addOne).followedBy).l
    traversedNodes.size shouldBe 16
    results.size shouldBe 0
  }

  "emit everything along the way if so configured" in {
    centerTrav.repeat(_.followedBy)(_.emit).name.toSet shouldBe Set("L3", "L2", "L1", "Center", "R1", "R2", "R3", "R4")
    centerTrav.repeat(_.out)(_.emit).property("name").toSet shouldBe Set("L3", "L2", "L1", "Center", "R1", "R2", "R3", "R4")
  }

  "emit everything but the first element (starting point)" in {
    centerTrav.repeat(_.followedBy)(_.emitAllButFirst).name.toSet shouldBe Set("L3", "L2", "L1", "R1", "R2", "R3", "R4")
    centerTrav.repeat(_.out)(_.emitAllButFirst).property("name").toSet shouldBe Set("L3", "L2", "L1", "R1", "R2", "R3", "R4")
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
