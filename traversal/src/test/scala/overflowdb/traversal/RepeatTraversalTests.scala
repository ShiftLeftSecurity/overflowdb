package overflowdb.traversal

import org.scalatest.{Matchers, WordSpec}
import overflowdb._
import overflowdb.traversal.testdomains.simple.Thing.Properties.Name
import overflowdb.traversal.testdomains.simple.{Connection, ExampleGraphSetup, SimpleDomain, Thing}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class RepeatTraversalTests extends WordSpec with Matchers {
  import ExampleGraphSetup._
  /* most tests work with this simple graph:
   * L3 <- L2 <- L1 <- Center -> R1 -> R2 -> R3 -> R4 -> R5
   */

  "typical case for both domain-specific steps" in {
    centerTrav.repeat(_.followedBy)(_.times(2)).name.toSet shouldBe Set("L2", "R2")
  }

  "typical case for both generic graph steps" in {
    centerTrav.repeat(_.out)(_.times(2)).property(Name).toSet shouldBe Set("L2", "R2")
  }

  "repeat given traversal X times" should {
    "return only the final elements" in {
      val expectedResults = Set("L2", "R2")
      centerTrav.repeat(_.followedBy)(_.times(2)).name.toSet shouldBe expectedResults
      centerTrav.repeat(_.followedBy)(_.times(2).breadthFirstSearch).name.toSet shouldBe expectedResults
      centerTrav.repeat(_.out)(_.times(2)).property(Name).toSet shouldBe expectedResults
      centerTrav.repeat(_.out)(_.times(2).breadthFirstSearch).property(Name).toSet shouldBe expectedResults
    }

    "return only the final elements - if any" in {
      val expectedResults = Set("R4") // there is no L4
      centerTrav.repeat(_.followedBy)(_.times(4)).name.toSet shouldBe expectedResults
      centerTrav.repeat(_.followedBy)(_.times(4).breadthFirstSearch).name.toSet shouldBe expectedResults
      centerTrav.repeat(_.out)(_.times(4)).property(Name).toSet shouldBe expectedResults
      centerTrav.repeat(_.out)(_.times(4).breadthFirstSearch).property(Name).toSet shouldBe expectedResults
    }

    "return everything along the way also, if used in combination with emit" in {
      val expectedResults = Set("Center", "L1", "L2", "R1", "R2")
      centerTrav.repeat(_.followedBy)(_.times(2).emit).name.toSet shouldBe expectedResults
      centerTrav.repeat(_.followedBy)(_.times(2).emit.breadthFirstSearch).name.toSet shouldBe expectedResults
      centerTrav.repeat(_.out)(_.times(2).emit).property(Name).toSet shouldBe expectedResults
      centerTrav.repeat(_.out)(_.times(2).emit.breadthFirstSearch).property(Name).toSet shouldBe expectedResults
    }
  }

  "emit everything along the way if so configured" in {
    val expectedResults = Set("L3", "L2", "L1", "Center", "R1", "R2", "R3", "R4", "R5")
    centerTrav.repeat(_.followedBy)(_.emit).name.toSet shouldBe expectedResults
    centerTrav.repeat(_.followedBy)(_.emit.breadthFirstSearch).name.toSet shouldBe expectedResults
    centerTrav.repeat(_.out)(_.emit).property("name").toSet shouldBe expectedResults
    centerTrav.repeat(_.out)(_.emit.breadthFirstSearch).property("name").toSet shouldBe expectedResults
  }

  "emit everything but the first element (starting point)" in {
    val expectedResults = Set("L3", "L2", "L1", "R1", "R2", "R3", "R4", "R5")
    centerTrav.repeat(_.followedBy)(_.emitAllButFirst).name.toSet shouldBe expectedResults
    centerTrav.repeat(_.followedBy)(_.emitAllButFirst.breadthFirstSearch).name.toSet shouldBe expectedResults
    centerTrav.repeat(_.out)(_.emitAllButFirst).property("name").toSet shouldBe expectedResults
    centerTrav.repeat(_.out)(_.emitAllButFirst.breadthFirstSearch).property("name").toSet shouldBe expectedResults
  }

  "emit nodes that meet given condition" in {
    val expectedResults = Set("L1", "L2", "L3")
    centerTrav.repeat(_.followedBy)(_.emit(_.name.filter(_.startsWith("L")))).name.toSet shouldBe expectedResults
    centerTrav.repeat(_.followedBy)(_.emit(_.name.filter(_.startsWith("L"))).breadthFirstSearch).name.toSet shouldBe expectedResults
    centerTrav.repeat(_.out)(_.emit(_.has(Name.where(_.startsWith("L"))))).property(Name).toSet shouldBe expectedResults
    centerTrav.repeat(_.out)(_.emit(_.has(Name.where(_.startsWith("L")))).breadthFirstSearch).property(Name).toSet shouldBe expectedResults
  }

  "support arbitrary `until` condition" should {
    "work without emit" in {
      val expectedResults = Set("L2", "R2")
      centerTrav.repeat(_.followedBy)(_.until(_.name.filter(_.endsWith("2")))).name.toSet shouldBe expectedResults
      centerTrav.repeat(_.followedBy)(_.until(_.name.filter(_.endsWith("2"))).breadthFirstSearch).name.toSet shouldBe expectedResults

      centerTrav.repeat(_.out)(
        _.until(_.has(Name.where(_.matches(".*2")))))
        .property(Name).toSet shouldBe expectedResults

      centerTrav.repeat(_.out)(
        _.until(_.has(Name.where(_.matches(".*2")))).breadthFirstSearch)
        .property(Name).toSet shouldBe expectedResults
    }

    "work in combination with emit" in {
      val expectedResults = Set("Center", "L1", "L2", "R1", "R2")
      centerTrav.repeat(_.followedBy)(_.until(_.name.filter(_.endsWith("2"))).emit).name.toSet shouldBe expectedResults
      centerTrav.repeat(_.followedBy)(_.until(_.name.filter(_.endsWith("2"))).emit.breadthFirstSearch).name.toSet shouldBe expectedResults
      centerTrav.repeat(_.out)(_.until(_.has(Name.where(_.matches(".*2")))).emit).property(Name).toSet shouldBe expectedResults
      centerTrav.repeat(_.out)(_.until(_.has(Name.where(_.matches(".*2")))).emit.breadthFirstSearch).property(Name).toSet shouldBe expectedResults
    }

    "result in 'repeat/until' behaviour, i.e. `until` condition is only evaluated after one iteration" in {
      val expectedResults = Set("L1", "R1")
      centerTrav.repeat(_.followedBy)(_.until(_.filter(_.label == Thing.Label))).name.toSet shouldBe expectedResults
      centerTrav.repeat(_.followedBy)(_.until(_.filter(_.label == Thing.Label)).breadthFirstSearch).name.toSet shouldBe expectedResults
      centerTrav.repeat(_.out)(_.until(_.hasLabel(Thing.Label))).property(Name).toSet shouldBe expectedResults
      centerTrav.repeat(_.out)(_.until(_.hasLabel(Thing.Label)).breadthFirstSearch).property(Name).toSet shouldBe expectedResults
    }

  }

  "is lazy" in {
    val traversedNodes = mutable.ListBuffer.empty[Node]
    val traversalNotYetExecuted = {
      centerTrav.repeat(_.followedBy.sideEffect(traversedNodes.addOne))
      centerTrav.repeat(_.followedBy.sideEffect(traversedNodes.addOne))(_.breadthFirstSearch)
      centerTrav.repeat(_.out.sideEffect(traversedNodes.addOne))
      centerTrav.repeat(_.out.sideEffect(traversedNodes.addOne))(_.breadthFirstSearch)
    }
    withClue("traversal should not do anything when it's only created") {
      traversedNodes.size shouldBe 0
    }
  }

  "hasNext check doesn't change contents of traversal" when {
    "path tracking is not enabled" in {
      val trav = centerTrav.repeat(_.followedBy)(_.times(2))
      trav.hasNext shouldBe true
      trav.toSet shouldBe Set(l2, r2)
    }

    "path tracking not enabled" in {
      val trav = centerTrav.enablePathTracking.repeat(_.followedBy)(_.times(2)).path
      trav.hasNext shouldBe true
      trav.toSet shouldBe Set(
        Seq(center, l1, l2),
        Seq(center, r1, r2)
      )
    }
  }

  "traverses all nodes to outer limits exactly once, emitting and returning nothing, by default" in {
    val traversedNodes = mutable.ListBuffer.empty[Thing]
    def test(traverse: => Iterable[_]) = {
      traversedNodes.clear
      val results = traverse
      traversedNodes.size shouldBe 9
      results.size shouldBe 0
    }

    test(centerTrav.repeat(_.sideEffect(traversedNodes.addOne).followedBy).l)
    test(centerTrav.repeat(_.sideEffect(traversedNodes.addOne).followedBy).l)
    test(centerTrav.repeat(_.sideEffect(traversedNodes.addOne).followedBy)(_.breadthFirstSearch).l)
    test(centerTrav.repeat(_.sideEffect(traversedNodes.addOne).out).l)
    test(centerTrav.repeat(_.sideEffect(traversedNodes.addOne).out)(_.breadthFirstSearch).l)

    withClue("for reference: this behaviour is adapted from tinkerpop") {
      import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.__
      import org.apache.tinkerpop.gremlin.process.traversal.{Traverser, Traversal => TPTraversal}
      test(
        __(center).repeat(
          __().sideEffect { x: Traverser[Thing] => traversedNodes += x.get }
            .out().asInstanceOf[TPTraversal[_, Thing]]
        ).toList.asScala)
    }
  }

  "uses DFS (depth first search) by default" in {
    val traversedNodes = mutable.ListBuffer.empty[Thing]
    centerTrav.repeat(_.sideEffect(traversedNodes.addOne).followedBy).iterate

    traversedNodes.map(_.name).toList shouldBe List("Center", "L1", "L2", "L3", "R1", "R2", "R3", "R4", "R5")
  }

  "uses BFS (breadth first search) if configured" in {
    val traversedNodes = mutable.ListBuffer.empty[Thing]
    centerTrav.repeat(_.sideEffect(traversedNodes.addOne).followedBy)(_.breadthFirstSearch).iterate

    traversedNodes.map(_.name).toList shouldBe List("Center", "L1", "R1", "L2", "R2", "L3", "R3", "R4", "R5")
  }

  "hasNext is idempotent: DFS" in {
    val traversedNodes = mutable.ListBuffer.empty[Thing]
    val traversal = centerTrav.repeat(_.sideEffect(traversedNodes.addOne).followedBy)(_.times(3))

    // hasNext will run the provided repeat traversal exactly 3 times (as configured)
    traversal.hasNext shouldBe true
    traversedNodes.size shouldBe 3
    traversedNodes.map(_.name).toList shouldBe List("Center", "L1", "L2")
    // hasNext is idempotent - calling it again doesn't result in any further traversing
    traversal.hasNext shouldBe true
    traversedNodes.size shouldBe 3
  }

  "hasNext is idempotent: BFS" in {
    val traversedNodes = mutable.ListBuffer.empty[Thing]
    val traversal = centerTrav.repeat(_.sideEffect(traversedNodes.addOne).followedBy)(_.times(2).breadthFirstSearch)

    // hasNext will run the provided repeat traversal exactly 3 times (as configured)
    traversal.hasNext shouldBe true
    traversedNodes.size shouldBe 2
    traversedNodes.map(_.name).toList shouldBe List("Center", "L1")
    // hasNext is idempotent - calling it again doesn't result in any further traversing
    traversal.hasNext shouldBe true
    traversedNodes.size shouldBe 2
  }

  "supports large amount of iterations" in {
    // using circular graph so that we can repeat any number of times
    val graph = SimpleDomain.newGraph

    def addThing(name: String) = graph + (Thing.Label, Name -> name)

    val a = addThing("a")
    val b = addThing("b")
    val c = addThing("c")
    a --- Connection.Label --> b
    b --- Connection.Label --> c
    c --- Connection.Label --> a

    val repeatCount = 100000
    Traversal.fromSingle(a).repeat(_.out)(_.times(repeatCount)).property(Name).l shouldBe List("b")
    Traversal.fromSingle(a).repeat(_.out)(_.times(repeatCount).breadthFirstSearch).property(Name).l shouldBe List("b")

    // for reference: tinkerpop becomes very slow with large iteration counts:
    // on my machine this didn't terminate within 5mins, hence commenting out
//    import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.__
//    __(a).repeat(
//      __().out().asInstanceOf[org.apache.tinkerpop.gremlin.process.traversal.Traversal[_, Node]]
//    ).times(repeatCount).values[String](Name.name).next() shouldBe "b"
  }
}
