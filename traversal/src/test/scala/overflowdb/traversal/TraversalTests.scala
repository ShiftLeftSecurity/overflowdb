package overflowdb.traversal

import org.scalatest.{Matchers, WordSpec}
import overflowdb.traversal.testdomains.simple.{ExampleGraphSetup, Thing}

import scala.collection.mutable

//object Foo extends App {
//  import ExampleGraphSetup._
//  r1.start.enablePathTracking.repeat(_.out)(_.emit).foreach(println) //R1 R2 R3 R4 R5 -> ok
//  r1.start.enablePathTracking.repeat(_.out)(_.emit).path.foreach(println) // ok as well...
//  r1.start.enablePathTracking.repeat(_.out.out)(_.emit).foreach(println) // R1 R3 R5 -> ok
//  r1.start.enablePathTracking.repeat(_.out.out)(_.emit).path.foreach(println) // R1(R1) - R3(R1 R3) - R5(R1 R3 R5) -> also ok!

//  r1.start.enablePathTracking.repeat{n: Traversal[Thing] =>
//    val n1 = n.next
//    n1.start.enablePathTracking.out.out
////    n.out.out
//  }(_.emit).path.foreach(println)

//  center.start.enablePathTracking.repeat(_.out)(_.emit).path.foreach(println)

//  centerTrav.enablePathTracking.repeat(_.out)(_.emit).path.foreach(println)
//    Seq(center, l1, l2, l3),
//    Seq(center, r1, r2, r3, r4, r5))
//}

class TraversalTests extends WordSpec with Matchers {
  import ExampleGraphSetup._

  "can only be iterated once" in {
    val one = Traversal.fromSingle("one")
    one.size shouldBe 1
    one.size shouldBe 0

    val empty = Traversal(Nil)
    empty.size shouldBe 0
    empty.size shouldBe 0
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
    simpleDomain.all.property(Thing.Properties.Name).toSet shouldBe Set("L3", "L2", "L1", "Center", "R1", "R2", "R3", "R4", "R5")
    centerTrav.head.name shouldBe "Center"
    simpleDomain.all.label.toSet shouldBe Set(Thing.Label)
  }

  ".dedup step" should {
    "remove duplicates: simple scenario" in {
      Traversal(Iterator(1,2,1,3)).dedup.l shouldBe List(1,2,3)
      Traversal(Iterator(1,2,1,3)).dedup(_.hashComparisonOnly).l shouldBe List(1,2,3)
    }

    "allow method only based on hashCode - to ensure the traversal doesn't hold onto elements after they've been consumed" in {
      // when run with -Xmx128m we can hold ~7 of these at a time
      def infiniteTraversalWithLargeElements = Traversal(new Iterator[Any] {
        var i = 0
        def hasNext = true
        def next(): Any = {
          val arr = Array.ofDim[Long](2048, 1024)
          // assign unique value to make the arrays unique
          arr(i)(i) = i
          i += 1
          arr
        }
      })

      /** using dedup by hash comparison, we can traverse over these elements - already consumed elements are garbage collected */
      val traversal = infiniteTraversalWithLargeElements.dedup(_.hashComparisonOnly)
      0.to(128).foreach { i =>
        traversal.next
      }

      /** This is a copy of the above, but using the default dedup comparison style (hashAndEquals). To be able to
       * compare objects via `.equals` it has to hold onto already consumed objects, making it run out of memory
       * eventually. When run with -Xmx128m this happens after ~5 iterations. */
//      val traversal2 = infiniteTraversalWithLargeElements.dedup
//      0.to(128).foreach { i =>
//        println(i)
//        traversal2.next
//      }
    }
  }

  ".path step" should {
    "not be enabled by default" in {
      intercept[AssertionError] { centerTrav.out.path }
    }

    "work for single element traversal (boring)" in {
      centerTrav.enablePathTracking.path.toSet shouldBe Set(Seq(center))
    }

    "work for simple one-step expansion" in {
      centerTrav.enablePathTracking.out.path.toSet shouldBe Set(
        Seq(center, l1),
        Seq(center, r1))
    }

    "work for simple two-step expansion" in {
      centerTrav.enablePathTracking.out.out.path.toSet shouldBe Set(
        Seq(center, l1, l2),
        Seq(center, r1, r2))
    }

    "only track from where it's enabled" in {
      centerTrav.out.enablePathTracking.out.path.toSet shouldBe Set(
        Seq(l1, l2),
        Seq(r1, r2))
    }

    "support domain-specific steps" in {
      centerTrav.enablePathTracking.followedBy.followedBy.path.toSet shouldBe Set(
        Seq(center, l1, l2),
        Seq(center, r1, r2))
    }

    "work in combination with other steps" should {

      ".map: include intermediate results in path" in {
          centerTrav.enablePathTracking.followedBy.map(_.name).path.toSet shouldBe Set(
            Seq(center, l1, "L1"),
            Seq(center, r1, "R1"))
      }

      "collect: include intermediate results in path" in {
        centerTrav.enablePathTracking.followedBy.collect { case x => x.name }.path.toSet shouldBe Set(
          Seq(center, l1, "L1"),
          Seq(center, r1, "R1"))
      }

      "filter" in {
        centerTrav.enablePathTracking.followedBy.nameStartsWith("R").followedBy.path.toSet shouldBe Set(
          Seq(center, r1, r2))
      }

      "filterNot" in {
        centerTrav.enablePathTracking.followedBy.filterNot(_.name.startsWith("R")).followedBy.path.toSet shouldBe Set(
          Seq(center, l1, l2))
      }

      "where" in {
        centerTrav.enablePathTracking.followedBy.where(_.nameStartsWith("R")).followedBy.path.toSet shouldBe Set(
          Seq(center, r1, r2))
      }

      "whereNot" in {
        centerTrav.enablePathTracking.followedBy.whereNot(_.nameStartsWith("R")).followedBy.path.toSet shouldBe Set(
          Seq(center, l1, l2))
      }
    }

    "support repeat step" when {
      "using `times` modulator" in {
        centerTrav.enablePathTracking.repeat(_.out)(_.times(2)).path.toSet shouldBe Set(
          Seq(center, l1, l2),
          Seq(center, r1, r2))
      }

      "using `emit` modulator" in {
        centerTrav.enablePathTracking.repeat(_.out)(_.emit).path.toSet shouldBe Set(
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
        centerTrav.enablePathTracking.repeat(_.followedBy)(_.until(_.nameEndsWith("2"))).path.toSet shouldBe Set(
          Seq(center, l1, l2),
          Seq(center, r1, r2))
      }

      "using breadth first search" in {
        centerTrav.enablePathTracking.repeat(_.followedBy)(_.breadthFirstSearch.times(2)).path.toSet shouldBe Set(
          Seq(center, l1, l2),
          Seq(center, r1, r2))
      }

      "doing multiple steps: should track every single step along the way" in {
        centerTrav.enablePathTracking.repeat(_.followedBy.followedBy)(_.times(1)).path.toSet shouldBe Set(
          Seq(center, l1, l2),
          Seq(center, r1, r2))

        r1.start.enablePathTracking.repeat(_.followedBy.followedBy.followedBy)(_.times(1)).path.toSet shouldBe Set(
          Seq(r1, r2, r3, r4))

        r1.start.enablePathTracking.repeat(_.out.out)(_.times(2)).l shouldBe Seq(r5)
        r1.start.enablePathTracking.repeat(_.out.out)(_.times(2)).path.head shouldBe List(r1, r2, r3, r4, r5)
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
