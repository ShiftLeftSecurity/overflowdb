package overflowdb.traversal

import org.scalatest.{Matchers, WordSpec}
import overflowdb.Node
import overflowdb.traversal.testdomains.simple.{ExampleGraphSetup, Thing}
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import Thing.Properties.Name

class RepeatTraversalTests extends WordSpec with Matchers {
  import ExampleGraphSetup._

  "typical case for both domain-specific steps" in {
    centerTrav.repeatX(_.followedBy)(_.times(2)).name.toSet shouldBe Set("L2", "R2")
  }

  "typical case for both generic graph steps" in {
    centerTrav.repeatX(_.out)(_.times(2)).property(Name).toSet shouldBe Set("L2", "R2")
  }

  "repeat given traversal X times" should {
    "return only the final elements" in {
      val expectedResults = Set("L2", "R2")
      centerTrav.repeatX(_.followedBy)(_.times(2)).name.toSet shouldBe expectedResults
      centerTrav.repeatX(_.followedBy)(_.times(2).breadthFirstSearch).name.toSet shouldBe expectedResults
      centerTrav.repeatX(_.out)(_.times(2)).property(Name).toSet shouldBe expectedResults
      centerTrav.repeatX(_.out)(_.times(2).breadthFirstSearch).property(Name).toSet shouldBe expectedResults
    }

    "return only the final elements - if any" in {
      val expectedResults = Set("R4") // there is no L4
      centerTrav.repeatX(_.followedBy)(_.times(4)).name.toSet shouldBe expectedResults
      centerTrav.repeatX(_.followedBy)(_.times(4).breadthFirstSearch).name.toSet shouldBe expectedResults
      centerTrav.repeatX(_.out)(_.times(4)).property(Name).toSet shouldBe expectedResults
      centerTrav.repeatX(_.out)(_.times(4).breadthFirstSearch).property(Name).toSet shouldBe expectedResults
    }

    "return everything along the way also, if used in combination with emit" in {
      val expectedResults = Set("Center", "L1", "L2", "R1", "R2")
      centerTrav.repeatX(_.followedBy)(_.times(2).emit).name.toSet shouldBe expectedResults
      centerTrav.repeatX(_.followedBy)(_.times(2).emit.breadthFirstSearch).name.toSet shouldBe expectedResults
      centerTrav.repeatX(_.out)(_.times(2).emit).property(Name).toSet shouldBe expectedResults
      centerTrav.repeatX(_.out)(_.times(2).emit.breadthFirstSearch).property(Name).toSet shouldBe expectedResults
    }
  }

  "emit everything along the way if so configured" in {
    val expectedResults = Set("L3", "L2", "L1", "Center", "R1", "R2", "R3", "R4")
    centerTrav.repeatX(_.followedBy)(_.emit).name.toSet shouldBe expectedResults
    centerTrav.repeatX(_.followedBy)(_.emit.breadthFirstSearch).name.toSet shouldBe expectedResults
    centerTrav.repeatX(_.out)(_.emit).property("name").toSet shouldBe expectedResults
    centerTrav.repeatX(_.out)(_.emit.breadthFirstSearch).property("name").toSet shouldBe expectedResults
  }

  "emit everything but the first element (starting point)" in {
    val expectedResults = Set("L3", "L2", "L1", "R1", "R2", "R3", "R4")
    centerTrav.repeatX(_.followedBy)(_.emitAllButFirst).name.toSet shouldBe expectedResults
    centerTrav.repeatX(_.followedBy)(_.emitAllButFirst.breadthFirstSearch).name.toSet shouldBe expectedResults
    centerTrav.repeatX(_.out)(_.emitAllButFirst).property("name").toSet shouldBe expectedResults
    centerTrav.repeatX(_.out)(_.emitAllButFirst.breadthFirstSearch).property("name").toSet shouldBe expectedResults
  }

  "emit nodes that meet given condition" in {
    val expectedResults = Set("L1", "L2", "L3")
    centerTrav.repeatX(_.followedBy)(_.emit(_.name.startsWith("L"))).name.toSet shouldBe expectedResults
    centerTrav.repeatX(_.followedBy)(_.emit(_.name.startsWith("L")).breadthFirstSearch).name.toSet shouldBe expectedResults
    centerTrav.repeatX(_.out)(_.emit(_.property(Name).startsWith("L"))).property(Name).toSet shouldBe expectedResults
    centerTrav.repeatX(_.out)(_.emit(_.property(Name).startsWith("L")).breadthFirstSearch).property(Name).toSet shouldBe expectedResults
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
      centerTrav.repeat(_.out)(_.until(_.property(Name).endsWith("2")).emit).property(Name).toSet shouldBe Set("Center", "L1", "L2", "R1", "R2")
    }
  }

  "uses DFS (depth first search) by default" ignore {
    // TODO detailed step analysis as at the bottom
    ???
  }

  "uses DFS (depth first search) if configured" ignore {
    // TODO detailed step analysis as at the bottom
    ???
  }

  "is lazy" in {
    val traversedNodes = mutable.ListBuffer.empty[Node]
    val traversalNotYetExecuted = {
      centerTrav.repeatX(_.followedBy.sideEffect(traversedNodes.addOne))
      centerTrav.repeatX(_.followedBy.sideEffect(traversedNodes.addOne))(_.breadthFirstSearch)
      centerTrav.repeatX(_.out.sideEffect(traversedNodes.addOne))
      centerTrav.repeatX(_.out.sideEffect(traversedNodes.addOne))(_.breadthFirstSearch)
    }
    withClue("traversal should not do anything when it's only created") {
      traversedNodes.size shouldBe 0
    }
  }

  "traverses all nodes to outer limits exactly once, emitting and returning nothing, by default" in {
    val traversedNodes = mutable.ListBuffer.empty[Thing]
    def test(traverse: => Iterable[_]) = {
      traversedNodes.clear
      val results = traverse
      traversedNodes.size shouldBe 8
      results.size shouldBe 0
    }

    test(centerTrav.repeat(_.sideEffect(traversedNodes.addOne).out).l)
    test(centerTrav.repeat(_.sideEffect(traversedNodes.addOne).out)(_.breadthFirstSearch).l)
    test(centerTrav.repeat(_.sideEffect(traversedNodes.addOne).followedBy).l)
    test(centerTrav.repeat(_.sideEffect(traversedNodes.addOne).followedBy)(_.breadthFirstSearch).l)

    withClue("for reference: this behaviour is adapted from tinkerpop") {
      import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.__
      import org.apache.tinkerpop.gremlin.process.traversal.Traverser
      import org.apache.tinkerpop.gremlin.process.traversal.{Traversal => TPTraversal}
      test(
        __(centerNode).repeat(
          __().sideEffect { x: Traverser[Thing] => traversedNodes += x.get }
            .out().asInstanceOf[TPTraversal[_, Thing]]
        ).toList.asScala)
    }
  }

}

// TODO
//withClue("asserting more fine-grained traversal characteristics") {
//  val traversedNodes = mutable.ListBuffer.empty[Thing]
//  val traversal = centerTrav.repeat(_.sideEffect(traversedNodes.addOne).followedBy)(_.times(2)).name
//
//  // hasNext will run the provided repeat traversal exactly 2 times (as configured)
//  traversal.hasNext shouldBe true
//  traversedNodes.size shouldBe 2
//  // hasNext is idempotent
//  traversal.hasNext shouldBe true
//  traversedNodes.size shouldBe 2
//
//  traversal.next shouldBe "L2"
//  traversal.next shouldBe "R2"
//  traversedNodes.size shouldBe 3
//  traversedNodes.map(_.name).to(Set) shouldBe Set("Center", "L1", "R1")
//  traversal.hasNext shouldBe false
//}
