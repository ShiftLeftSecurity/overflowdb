package overflowdb.traversal

import org.scalatest.{Matchers, WordSpec}
import overflowdb._
import overflowdb.traversal.testdomains.simple.{Connection, ExampleGraphSetup, SimpleDomain, Thing}

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import Thing.Properties.Name

class RepeatTraversalTests extends WordSpec with Matchers {
  import ExampleGraphSetup._
  /* most tests work with this simple graph:
   * L3 <- L2 <- L1 <- Center -> R1 -> R2 -> R3 -> R4
   */

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
      val expectedResults = Set("L2", "R2")
      centerTrav.repeatX(_.followedBy)(_.until(_.name.endsWith("2"))).name.toSet shouldBe expectedResults
      centerTrav.repeatX(_.followedBy)(_.until(_.name.endsWith("2")).breadthFirstSearch).name.toSet shouldBe expectedResults
      centerTrav.repeatX(_.out)(_.until(_.property(Name).endsWith("2"))).property(Name).toSet shouldBe expectedResults
      centerTrav.repeatX(_.out)(_.until(_.property(Name).endsWith("2")).breadthFirstSearch).property(Name).toSet shouldBe expectedResults
    }

    "targeting the start element (edge case)" in {
      val expectedResults = Set("Center")
      centerTrav.repeatX(_.followedBy)(_.until(_.name == "Center")).name.toSet shouldBe expectedResults
      centerTrav.repeatX(_.followedBy)(_.until(_.name == "Center").breadthFirstSearch).name.toSet shouldBe expectedResults
      centerTrav.repeatX(_.out)(_.until(_.property(Name) == "Center")).property(Name).toSet shouldBe expectedResults
      centerTrav.repeatX(_.out)(_.until(_.property(Name) == "Center").breadthFirstSearch).property(Name).toSet shouldBe expectedResults
    }

    "used in combination with emit" in {
      val expectedResults = Set("Center", "L1", "L2", "R1", "R2")
      centerTrav.repeatX(_.followedBy)(_.until(_.name.endsWith("2")).emit).name.toSet shouldBe expectedResults
      centerTrav.repeatX(_.followedBy)(_.until(_.name.endsWith("2")).emit.breadthFirstSearch).name.toSet shouldBe expectedResults
      centerTrav.repeatX(_.out)(_.until(_.property(Name).endsWith("2")).emit).property(Name).toSet shouldBe expectedResults
      centerTrav.repeatX(_.out)(_.until(_.property(Name).endsWith("2")).emit.breadthFirstSearch).property(Name).toSet shouldBe expectedResults
    }
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

    test(centerTrav.repeatX{ node => traversedNodes.addOne(node); node.followedBy}.l)
    test(centerTrav.repeatX{ node => traversedNodes.addOne(node); node.followedBy}(_.breadthFirstSearch).l)
    test(centerTrav.repeatX{ node => traversedNodes.addOne(node); node.out}.l)
    test(centerTrav.repeatX{ node => traversedNodes.addOne(node); node.out}(_.breadthFirstSearch).l)

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

  "uses DFS (depth first search) by default" in {
    // asserting exact traversed path, in order to verify it's really DFS
    val traversedNodeNames = mutable.ListBuffer.empty[String]
    val traversal = centerTrav.repeatX { thing =>
      traversedNodeNames.addOne(thing.name)
      thing.followedBy
    }(_.times(3)).name

    // hasNext will run the provided repeat traversal exactly 3 times (as configured)
    traversal.hasNext shouldBe true
    traversedNodeNames.size shouldBe 3
    traversedNodeNames.toList shouldBe List("Center", "L1", "L2")
    traversal.next shouldBe "L3"

    traversal.hasNext shouldBe true
    traversedNodeNames.size shouldBe 5
    traversedNodeNames.toList shouldBe List("Center", "L1", "L2", "R1", "R2")
    traversal.next shouldBe "R3"

    traversal.hasNext shouldBe false
  }

  "uses BFS (breadth first search) if configured" in {
    // asserting exact traversed path, in order to verify it's really DFS
    val traversedNodeNames = mutable.ListBuffer.empty[String]
    val traversal = centerTrav.repeatX { thing =>
      traversedNodeNames.addOne(thing.name)
      thing.followedBy
    }(_.times(3).breadthFirstSearch).name

    // hasNext will run the provided repeat traversal exactly 3 times (as configured)
    traversal.hasNext shouldBe true
    traversedNodeNames.size shouldBe 5
    traversedNodeNames.toList shouldBe List("Center", "L1", "R1", "L2", "R2")
    traversal.next shouldBe "L3"

    traversal.hasNext shouldBe true
    traversedNodeNames.size shouldBe 5
    traversedNodeNames.toList shouldBe List("Center", "L1", "R1", "L2", "R2")
    traversal.next shouldBe "R3"

    traversal.hasNext shouldBe false
  }

  "hasNext is idempotent: DFS" in {
    val traversedNodeNames = mutable.ListBuffer.empty[String]
    val traversal = centerTrav.repeatX { thing =>
      traversedNodeNames.addOne(thing.name)
      thing.followedBy
    }(_.times(3))

    // hasNext will run the provided repeat traversal exactly 3 times (as configured)
    traversal.hasNext shouldBe true
    traversedNodeNames.size shouldBe 3
    traversedNodeNames.toList shouldBe List("Center", "L1", "L2")
    // hasNext is idempotent - calling it again doesn't result in any further traversing
    traversal.hasNext shouldBe true
    traversedNodeNames.size shouldBe 3
  }

  "hasNext is idempotent: BFS" in {
    val traversedNodeNames = mutable.ListBuffer.empty[String]
    val traversal = centerTrav.repeatX { thing =>
      traversedNodeNames.addOne(thing.name)
      thing.followedBy
    }(_.times(2).breadthFirstSearch)

    // hasNext will run the provided repeat traversal exactly 3 times (as configured)
    traversal.hasNext shouldBe true
    traversedNodeNames.size shouldBe 3
    traversedNodeNames.toList shouldBe List("Center", "L1", "R1")
    // hasNext is idempotent - calling it again doesn't result in any further traversing
    traversal.hasNext shouldBe true
    traversedNodeNames.size shouldBe 3
  }

  "supports arbitrary amount of iterations" when {
    // using circular graph so that we can repeat any number of times
    val graph = SimpleDomain.newGraph

    def addThing(name: String) = graph + (Thing.Label, Name -> name)

    val a = addThing("a")
    val b = addThing("b")
    val c = addThing("c")
    a --- Connection.Label --> b
    b --- Connection.Label --> c
    c --- Connection.Label --> a

    "repeating very very often" in {
      val repeatCount = 100000
      Traversal.fromSingle(a).repeatX(_.out)(_.times(repeatCount)).property(Name).l shouldBe List("b")
      Traversal.fromSingle(a).repeatX(_.out)(_.times(repeatCount).breadthFirstSearch).property(Name).l shouldBe List("b")

      // for reference: tinkerpop becomes very slow with large iteration counts:
      // on my machine this didn't terminate within 5mins, hence commenting out
      //      import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.__
      //      __(a).repeat(
      //        __().out().asInstanceOf[org.apache.tinkerpop.gremlin.process.traversal.Traversal[_, Node]]
      //      ).times(repeatCount).values[String](Name.name).next() shouldBe "b"
    }

    "foo2" in {
      val repeatCount = 9000
      //      val repeatCount = 900
      val trav = Traversal.fromSingle(a).repeatDfs2(_.out, new RepeatBehaviour.Builder[Node].times(repeatCount).build)
      //      val trav = Traversal.fromSingle(a).repeat8(_.out)(_.times(repeatCount))
      println("first: " + trav.next)
      trav.hasNext //stackoverflow
    }

    "foo3" in {
      val repeatCount = 2
      val trav = Traversal.fromSingle(a).repeatDfs3(_.out, new RepeatBehaviour.Builder[Node].times(repeatCount).build).property(Name)
      println("hasNext: " + trav.hasNext)
      //      println("first: " + trav.next)
      //      println("hasNext: " + trav.hasNext)
    }

    "foo4" in {
      centerTrav.foreach { center =>
        println(center.property(Name))
        center.out.foreach { one =>
          println(one.property(Name))
          one.out.foreach { two =>
            println(two.property(Name))
          }
        }
      }
      // this is DFS: Center, L1, L2, R1, R2
      // now make it lazy
    }

    "foo5 - this works!" in {
      type Depth = Int
      val stack: mutable.Stack[(Traversal[Thing], Depth)] = mutable.Stack.empty
      stack.push((centerTrav, 0))

      val untilDepth = 3

      val iter = new Iterator[Thing] {

        override def hasNext: Boolean = {
          while (stack.nonEmpty) {
            val (trav, depth) = stack.top
            if (trav.isEmpty) stack.pop()
            else if (depth == untilDepth) return true
            else {
              val element = trav.next
              println(s"traversing over $element")
              stack.push((Traversal.fromSingle(element).followedBy, depth + 1))
            }
          }
          false
        }

        override def next(): Thing = {
          if (hasNext) {
            // at this point it's guaranteed to have the next non-empty traversal at the top of the stack
            stack.top._1.next
          } else {
            throw new NoSuchElementException("next on empty iterator")
          }
        }
      }

      println(iter.next)
      println(iter.next)
      println(iter.hasNext)
    }

    "foo6 - try with circular graph" in {
      // using circular graph so that we can repeat any number of times
      val graph = SimpleDomain.newGraph

      def addThing(name: String) = graph + (Thing.Label, Name -> name)

      val a = addThing("a")
      val b = addThing("b")
      val c = addThing("c")
      a --- Connection.Label --> b
      b --- Connection.Label --> c
      c --- Connection.Label --> a

//      val repeatCount = 2
//      val repeatCount = 20
      val repeatCount = 100000
//      Traversal.fromSingle(a).repeatX(_.out)(_.times(repeatCount)).property(Name).l shouldBe List("b")
//        Traversal.fromSingle(a).repeatX(_.out)(_.times(repeatCount).breadthFirstSearch).property(Name).l shouldBe List("b")

      type Depth = Int
      val stack: mutable.Stack[(Traversal[Node], Depth)] = mutable.Stack.empty
      val startTraversal = Traversal.fromSingle(a)
      stack.push((startTraversal, 0))

      val iter = new Iterator[Node] {

        override def hasNext: Boolean = {
          while (stack.nonEmpty) {
            val (trav, depth) = stack.top
            if (trav.isEmpty) stack.pop()
            else if (depth == repeatCount) return true
            else {
              val element = trav.next
              println(s"traversing over $element")
              stack.push((Traversal.fromSingle(element).out, depth + 1))
            }
          }
          false
        }

        override def next(): Node = {
          if (hasNext) {
            // at this point it's guaranteed to have the next non-empty traversal at the top of the stack
            stack.top._1.next
          } else {
            throw new NoSuchElementException("next on empty iterator")
          }
        }
      }


      println(iter.toList)
//      println(iter.next)
//      println(iter.next)
//      println(iter.hasNext)
    }
  }

}
