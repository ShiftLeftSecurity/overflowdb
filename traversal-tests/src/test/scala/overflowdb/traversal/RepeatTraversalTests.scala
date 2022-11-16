package overflowdb.traversal

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import overflowdb._
import overflowdb.traversal.testdomains.simple.Thing.Properties.Name
import overflowdb.traversal.testdomains.simple.{Connection, ExampleGraphSetup, SimpleDomain, Thing, ThingTraversal}
import scala.collection.mutable

class RepeatTraversalTests extends AnyWordSpec with ExampleGraphSetup {

  /* most tests work with this simple graph:
   * L3 <- L2 <- L1 <- Center -> R1 -> R2 -> R3 -> R4 -> R5
   */
  "typical case for both domain-specific steps" in {
    centerTrav.repeat(_.followedBy)(_.maxDepth(2)).name.toSetMutable shouldBe Set("L2", "R2")
  }

  "typical case for both generic graph steps" in {
    centerTrav.repeat(_.out)(_.maxDepth(2)).property(Name).toSetMutable shouldBe Set("L2", "R2")
  }

  "repeat given traversal up to depth 2" should {
    "return only the final elements" in {
      val expectedResults = Set("L2", "R2")
      centerTrav.repeat(_.followedBy)(_.maxDepth(2)).name.toSetMutable shouldBe expectedResults
      centerTrav.repeat(_.followedBy)(_.maxDepth(2).breadthFirstSearch).name.toSetMutable shouldBe expectedResults
      centerTrav.repeat(_.out)(_.maxDepth(2)).property(Name).toSetMutable shouldBe expectedResults
      centerTrav.repeat(_.out)(_.maxDepth(2).breadthFirstSearch).property(Name).toSetMutable shouldBe expectedResults
    }

    "return only the final elements - if any" in {
      val expectedResults = Set("R4") // there is no L4
      centerTrav.repeat(_.followedBy)(_.maxDepth(4)).name.toSetMutable shouldBe expectedResults
      centerTrav.repeat(_.followedBy)(_.maxDepth(4).breadthFirstSearch).name.toSetMutable shouldBe expectedResults
      centerTrav.repeat(_.out)(_.maxDepth(4)).property(Name).toSetMutable shouldBe expectedResults
      centerTrav.repeat(_.out)(_.maxDepth(4).breadthFirstSearch).property(Name).toSetMutable shouldBe expectedResults
    }

    "return everything along the way also, if used in combination with emit" in {
      val expectedResults = Set("Center", "L1", "L2", "R1", "R2")
      centerTrav.repeat(_.followedBy)(_.maxDepth(2).emit).name.toSetMutable shouldBe expectedResults
      centerTrav.repeat(_.followedBy)(_.maxDepth(2).emit.breadthFirstSearch).name.toSetMutable shouldBe expectedResults
      centerTrav.repeat(_.out)(_.maxDepth(2).emit).property(Name).toSetMutable shouldBe expectedResults
      centerTrav.repeat(_.out)(_.maxDepth(2).emit.breadthFirstSearch).property(Name).toSetMutable shouldBe expectedResults
    }
  }

  "emit everything along the way if so configured" in {
    val expectedResults = Set("L3", "L2", "L1", "Center", "R1", "R2", "R3", "R4", "R5")
    centerTrav.repeat(_.followedBy)(_.emit).name.toSetMutable shouldBe expectedResults
    centerTrav.repeat(_.followedBy)(_.emit.breadthFirstSearch).name.toSetMutable shouldBe expectedResults
    centerTrav.repeat(_.out)(_.emit).property("name").toSetMutable shouldBe expectedResults
    centerTrav.repeat(_.out)(_.emit.breadthFirstSearch).property("name").toSetMutable shouldBe expectedResults
  }

  "emit everything but the first element (starting point)" in {
    val expectedResults = Set("L3", "L2", "L1", "R1", "R2", "R3", "R4", "R5")
    centerTrav.repeat(_.followedBy)(_.emitAllButFirst).name.toSetMutable shouldBe expectedResults
    centerTrav.repeat(_.followedBy)(_.emitAllButFirst.breadthFirstSearch).name.toSetMutable shouldBe expectedResults
    centerTrav.repeat(_.out)(_.emitAllButFirst).property("name").toSetMutable shouldBe expectedResults
    centerTrav.repeat(_.out)(_.emitAllButFirst.breadthFirstSearch).property("name").toSetMutable shouldBe expectedResults
  }

  "emit nodes that meet given condition" in {
    val expectedResults = Set("L1", "L2", "L3")
    centerTrav.repeat(_.followedBy)(_.emit(_.nameStartsWith("L"))).name.toSetMutable shouldBe expectedResults
    centerTrav.repeat(_.followedBy)(_.emit(_.nameStartsWith("L")).breadthFirstSearch).name.toSetMutable shouldBe expectedResults
    centerTrav.repeat(_.out)(_.emit(_.has(Name.where(_.startsWith("L"))))).property(Name).toSetMutable shouldBe expectedResults
    centerTrav.repeat(_.out)(_.emit(_.has(Name.where(_.startsWith("L")))).breadthFirstSearch).property(Name).toSetMutable shouldBe expectedResults
  }

  "going through multiple steps in repeat traversal" in {
    r1.start.repeat(_.out.out)(_.emit).l shouldBe Seq(r1, r3, r5)
    r1.start.enablePathTracking.repeat(_.out.out)(_.emit).path.l shouldBe Seq(
      Seq(r1),
      Seq(r1, r2, r3),
      Seq(r1, r2, r3, r4, r5),
    )
  }

  "support arbitrary `until` condition" should {
    "work without emit" in {
      val expectedResults = Set("L2", "R2")
      centerTrav.repeat(_.followedBy)(_.until(_.nameEndsWith("2"))).name.toSetMutable shouldBe expectedResults
      centerTrav.repeat(_.followedBy)(_.until(_.nameEndsWith("2")).breadthFirstSearch).name.toSetMutable shouldBe expectedResults

      centerTrav.repeat(_.out)(
        _.until(_.has(Name.where(_.matches(".*2")))))
        .property(Name).toSetMutable shouldBe expectedResults

      centerTrav.repeat(_.out)(
        _.until(_.has(Name.where(_.matches(".*2")))).breadthFirstSearch)
        .property(Name).toSetMutable shouldBe expectedResults
    }

    "work in combination with emit" in {
      val expectedResults = Set("Center", "L1", "L2", "R1", "R2")
      centerTrav.repeat(_.followedBy)(_.until(_.nameEndsWith("2")).emit).name.toSetMutable shouldBe expectedResults
      centerTrav.repeat(_.followedBy)(_.until(_.nameEndsWith("2")).emit.breadthFirstSearch).name.toSetMutable shouldBe expectedResults
      centerTrav.repeat(_.out)(_.until(_.has(Name.where(_.matches(".*2")))).emit).property(Name).toSetMutable shouldBe expectedResults
      centerTrav.repeat(_.out)(_.until(_.has(Name.where(_.matches(".*2")))).emit.breadthFirstSearch).property(Name).toSetMutable shouldBe expectedResults
    }

    "result in 'repeat/until' behaviour, i.e. `until` condition is only evaluated after one iteration" in {
      val expectedResults = Set("L1", "R1")
      centerTrav.repeat(_.followedBy)(_.until(_.label(Thing.Label))).name.toSetMutable shouldBe expectedResults
      centerTrav.repeat(_.followedBy)(_.until(_.label(Thing.Label)).breadthFirstSearch).name.toSetMutable shouldBe expectedResults
      centerTrav.repeat(_.out)(_.until(_.hasLabel(Thing.Label))).property(Name).toSetMutable shouldBe expectedResults
      centerTrav.repeat(_.out)(_.until(_.hasLabel(Thing.Label)).breadthFirstSearch).property(Name).toSetMutable shouldBe expectedResults
    }

    "be combinable with `.maxDepth`" in {
      centerTrav.repeat(_.followedBy)(_.until(_.name("R2")).maxDepth(3)).name.toSetMutable shouldBe Set("L3", "R2")
    }
  }

  "until and maxDepth" should {
    "work in combination" in {
      centerTrav.repeat(_.out)(_.until(_.has(Name -> "R2")).maxDepth(2)).toSetMutable shouldBe Set(l2, r2)
      centerTrav.repeat(_.out)(_.until(_.has(Name -> "R2")).maxDepth(2)).has(Name -> "R2").l shouldBe Seq(r2)
      centerTrav.repeat(_.out)(_.breadthFirstSearch.until(_.has(Name -> "R2")).maxDepth(2)).toSetMutable shouldBe Set(l2, r2)
    }

    "work in combination with path" in {
      centerTrav.enablePathTracking.repeat(_.out)(_.until(_.has(Name -> "R2")).maxDepth(2)).path.filter(_.last == r2).l shouldBe Seq(Vector(center, r1, r2))
    }
  }

  "support repeat/while behaviour" should {
    "base case: given `whilst` condition is also evaluated for first iteration" in {
      centerTrav.repeat(_.followedBy)(_.whilst(_.name("does not exist"))).toSetMutable shouldBe Set(center)
      centerTrav.repeat(_.out)(_.whilst(_.has(Name, "does not exist"))).toSetMutable shouldBe Set(center)
    }

    "walk one iteration" in {
      centerTrav.repeat(_.followedBy)(_.whilst(_.name("Center"))).toSetMutable shouldBe Set(l1, r1)
      centerTrav.repeat(_.out)(_.whilst(_.has(Name, "Center"))).toSetMutable shouldBe Set(l1, r1)
    }

    "walk two iterations" in {
      centerTrav.repeat(_.followedBy)(_.whilst(_.or(
        _.name("Center"),
        _.nameEndsWith("1")
      ))).toSetMutable shouldBe Set(l2, r2)

      centerTrav.repeat(_.out)(_.whilst(_.or(
        _.has(Name.where(_.endsWith("1"))),
        _.has(Name, "Center"),
      ))).toSetMutable shouldBe Set(l2, r2)
    }

    "emitting nodes along the way" in {
      centerTrav.repeat(_.followedBy)(_.emit.whilst(_.name("Center"))).toSetMutable shouldBe Set(center, l1, r1)
      centerTrav.repeat(_.followedBy)(_.emitAllButFirst.whilst(_.name("Center"))).toSetMutable shouldBe Set(l1, r1)
    }

    "with path tracking enabled" in {
      centerTrav.enablePathTracking.repeat(_.followedBy)(_.whilst(_.name("Center"))).path.toSetMutable shouldBe Set(
        Seq(center, r1),
        Seq(center, l1),
      )
    }

    "be combinable with `.maxDepth`" in {
      centerTrav.repeat(_.followedBy)(_.whilst(_.nameNot("R2")).maxDepth(3)).name.toSetMutable shouldBe Set("L3", "R2")
    }
  }

  ".dedup should apply to all repeat iterations - e.g. to avoid cycles" when {
    "path tracking is not enabled" in {
      centerTrav.repeat(_.both)(_.maxDepth(2).dedup).toSetMutable shouldBe Set(l2, r2)
      centerTrav.repeat(_.both)(_.maxDepth(3).dedup).toSetMutable shouldBe Set(l3, r3)
      centerTrav.repeat(_.both)(_.maxDepth(4).dedup).toSetMutable shouldBe Set(r4)

      // for reference, without dedup (order is irrelevant, only using .l to show duplicate `center`)
      centerTrav.repeat(_.both)(_.maxDepth(2)).l shouldBe Seq(l2, center, r2, center)
    }

    "path tracking is enabled" in {
      centerTrav.enablePathTracking.repeat(_.both)(_.maxDepth(2).dedup).path.toSetMutable shouldBe Set(
        Seq(center, l1, l2),
        Seq(center, r1, r2)
      )

      // for reference, without dedup:
      centerTrav.enablePathTracking.repeat(_.both)(_.maxDepth(2)).path.toSetMutable shouldBe Set(
        Seq(center, l1, l2),
        Seq(center, l1, center),
        Seq(center, r1, r2),
        Seq(center, r1, center)
      )
    }

    "used with emit" in {
      // order is irrelevant, only using .l to show that there's no duplicates
      centerTrav.repeat(_.both)(_.maxDepth(2).emit.dedup).l shouldBe Seq(center, l1, l2, r1, r2)
    }

    "used with emit and path" in {
      centerTrav.enablePathTracking.repeat(_.both)(_.maxDepth(2).emit.dedup).path.toSetMutable shouldBe Set(
        Seq(center),
        Seq(center, l1),
        Seq(center, l1, l2),
        Seq(center, r1),
        Seq(center, r1, r2)
      )
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
      val trav = centerTrav.repeat(_.followedBy)(_.maxDepth(2))
      trav.hasNext shouldBe true
      trav.toSetMutable shouldBe Set(l2, r2)
    }

    "path tracking is enabled" in {
      val trav1 = centerTrav.enablePathTracking.repeat(_.followedBy)(_.maxDepth(2))
      val trav2 = centerTrav.enablePathTracking.repeat(_.followedBy)(_.maxDepth(2)).path
      trav1.hasNext shouldBe true
      trav2.hasNext shouldBe true
      trav1.toSetMutable shouldBe Set(l2, r2)
      trav2.toSetMutable shouldBe Set(
        Seq(center, l1, l2),
        Seq(center, r1, r2)
      )
    }
  }

  "traverses all nodes to outer limits exactly once, emitting and returning nothing, by default" in {
    val traversedNodes = mutable.ListBuffer.empty[Any]

    def test(traverse: => Iterable[_]) = {
      traversedNodes.clear()
      val results = traverse
      traversedNodes.size shouldBe 9
      results.size shouldBe 0
    }

    test(centerTrav.repeat(_.sideEffect(traversedNodes.addOne).followedBy).l)
    test(centerTrav.repeat(_.sideEffect(traversedNodes.addOne).followedBy).l)
    test(centerTrav.repeat(_.sideEffect(traversedNodes.addOne).followedBy)(_.breadthFirstSearch).l)
    test(centerTrav.repeat(_.sideEffect(traversedNodes.addOne).out).l)
    test(centerTrav.repeat(_.sideEffect(traversedNodes.addOne).out)(_.breadthFirstSearch).l)

    // for reference: this is the equivalent in tinkerpop - this doesn't compile any more because we dropped that dependency
    //    withClue("for reference: this behaviour is adapted from tinkerpop") {
    //      import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.__
    //      import org.apache.tinkerpop.gremlin.process.traversal.{Traverser, Traversal => TPTraversal}
    //      import org.apache.tinkerpop.gremlin.structure.Vertex
    //      test {
    //        val centerTp3: Vertex = NodeTp3.wrap(center)
    //        __(centerTp3).repeat(
    //          __().sideEffect { x: Traverser[Vertex] => traversedNodes += x.get }
    //            .out().asInstanceOf[TPTraversal[_, Vertex]]
    //        ).toList.asScala
    //      }
    //    }
  }

  "uses DFS (depth first search) by default" in {
    val traversedNodes = mutable.ListBuffer.empty[Thing]
    centerTrav.repeat(_.sideEffect(traversedNodes.addOne).followedBy).iterate()

    traversedNodes.map(_.name).toList shouldBe List("Center", "L1", "L2", "L3", "R1", "R2", "R3", "R4", "R5")
  }

  "uses BFS (breadth first search) if configured" in {
    val traversedNodes = mutable.ListBuffer.empty[Thing]
    centerTrav.repeat(_.sideEffect(traversedNodes.addOne).followedBy)(_.breadthFirstSearch).iterate()

    traversedNodes.map(_.name).toList shouldBe List("Center", "L1", "R1", "L2", "R2", "L3", "R3", "R4", "R5")
  }

  "hasNext is idempotent: DFS" in {
    val traversedNodes = mutable.ListBuffer.empty[Thing]
    val traversal = centerTrav.repeat(_.sideEffect(traversedNodes.addOne).followedBy)(_.maxDepth(3))

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
    val traversal = centerTrav.repeat(_.sideEffect(traversedNodes.addOne).followedBy)(_.maxDepth(2).breadthFirstSearch)

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

    def addThing(name: String) = graph + (Thing.Label, Name.of(name))

    val a = addThing("a")
    val b = addThing("b")
    val c = addThing("c")
    a --- Connection.Label --> b
    b --- Connection.Label --> c
    c --- Connection.Label --> a

    val repeatCount = 100000
    Traversal.fromSingle(a).repeat(_.out)(_.maxDepth(repeatCount)).property(Name).l shouldBe List("b")
    Traversal.fromSingle(a).repeat(_.out)(_.maxDepth(repeatCount).breadthFirstSearch).property(Name).l shouldBe List("b")

    // for reference: tinkerpop becomes very slow with large iteration counts:
    // on my machine this didn't terminate within 5mins, hence commenting out
    //    import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.__
    //    __(a).repeat(
    //      __().out().asInstanceOf[org.apache.tinkerpop.gremlin.process.traversal.Traversal[_, Node]]
    //    ).times(repeatCount).values[String](Name.name).next() shouldBe "b"
  }

  "support .path step" when {
    "using `maxDepth` modulator" in {
      centerTrav.enablePathTracking.repeat(_.out)(_.maxDepth(2)).path.toSetMutable shouldBe Set(
        Seq(center, l1, l2),
        Seq(center, r1, r2))
    }

    "using `emit` modulator" in {
      centerTrav.enablePathTracking.repeat(_.out)(_.emit).path.toSetMutable shouldBe Set(
        Seq(center),
        Seq(center, l1),
        Seq(center, l1, l2),
        Seq(center, l1, l2, l3),
        Seq(center, r1),
        Seq(center, r1, r2),
        Seq(center, r1, r2, r3),
        Seq(center, r1, r2, r3, r4),
        Seq(center, r1, r2, r3, r4, r5),
      )
    }

    "using `until` modulator" in {
      centerTrav.enablePathTracking.repeat(_.followedBy)(_.until(_.nameEndsWith("2"))).path.toSetMutable shouldBe Set(
        Seq(center, l1, l2),
        Seq(center, r1, r2))
    }

    "using breadth first search" in {
      centerTrav.enablePathTracking.repeat(_.followedBy)(_.breadthFirstSearch.maxDepth(2)).path.toSetMutable shouldBe Set(
        Seq(center, l1, l2),
        Seq(center, r1, r2))
    }

    "doing multiple steps: should track every single step along the way" in {
      centerTrav.enablePathTracking.repeat(_.followedBy.followedBy)(_.maxDepth(1)).path.toSetMutable shouldBe Set(
        Seq(center, l1, l2),
        Seq(center, r1, r2))

      r1.start.enablePathTracking.repeat(_.followedBy.followedBy.followedBy)(_.maxDepth(1)).path.toSetMutable shouldBe Set(
        Seq(r1, r2, r3, r4))

      r1.start.enablePathTracking.repeat(_.out.out)(_.maxDepth(2)).l shouldBe Seq(r5)
      r1.start.enablePathTracking.repeat(_.out.out)(_.maxDepth(2)).path.head shouldBe List(r1, r2, r3, r4, r5)
    }
  }

}
