package overflowdb.traversal

import org.scalatest.{Matchers, WordSpec}
import overflowdb.traversal.testdomains.simple.{ExampleGraphSetup, Thing}
import overflowdb.traversal.testdomains.gratefuldead._
import overflowdb.Node
import scala.collection.mutable

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

  ".sideEffect step should apply provided function and do nothing else" in {
    val sack = mutable.ListBuffer.empty[Node]
    center.start.out.sideEffect(sack.addOne).out.toSet shouldBe Set(l2, r2)
    sack.toSet shouldBe Set(l1, r1)
  }

  ".sideEffectPF step should support PartialFunction and not fail for undefined cases" in {
    val sack = mutable.ListBuffer.empty[Node]

    center
      .start
      .out
      .sideEffectPF {
        case node if node.property(Thing.Properties.Name).startsWith("L") =>
          sack.addOne(node)
      }
      .out
      .toSet shouldBe Set(l2, r2)

    sack.toSet shouldBe Set(l1)
  }

  "domain overview" in {
    simpleDomain.all.property(Thing.Properties.Name).toSet shouldBe Set("L3", "L2", "L1", "Center", "R1", "R2", "R3", "R4", "R5")
    centerTrav.head.name shouldBe "Center"
    simpleDomain.all.label.toSet shouldBe Set(Thing.Label)
  }

  ".dedup step" should {
    "remove duplicates" in {
      Traversal(Iterator(1,2,1,3)).dedup.l shouldBe List(1,2,3)
      Traversal(Iterator(1,2,1,3)).dedupBy(_.hashCode).l shouldBe List(1,2,3)
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
      val traversal = infiniteTraversalWithLargeElements.dedupBy(_.hashCode)
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

  "hasNext check doesn't change contents of traversal" when {
    "path tracking is not enabled" in {
      val trav = centerTrav.followedBy.followedBy
      trav.hasNext shouldBe true
      trav.toSet shouldBe Set(l2, r2)
    }

    "path tracking is enabled" in {
      val trav1 = centerTrav.enablePathTracking.followedBy.followedBy
      val trav2 = centerTrav.enablePathTracking.followedBy.followedBy.path
      trav1.hasNext shouldBe true
      trav2.hasNext shouldBe true
      trav1.toSet shouldBe Set(l2, r2)
      trav2.toSet shouldBe Set(
        Seq(center, l1, l2),
        Seq(center, r1, r2)
      )
    }
  }

  ".cast step should cast all elements to given type" in {
    val traversal: Traversal[Node] = center.start.out.out
    val results: Seq[Thing] = traversal.cast[Thing].l
    results shouldBe Seq(l2, r2)
  }

  ".collectAll step should collect (and cast) all elements of the given type" in {
    val graph = GratefulDead.newGraph
    val song = graph.addNode(Song.Label)
    val artist1 = graph.addNode(Artist.Label)
    val artist2 = graph.addNode(Artist.Label)

    val traversal: Traversal[Artist] = Traversal(graph.nodes).collectAll[Artist]
    traversal.l shouldBe Seq(artist1, artist2)
  }

  ".aggregate step stores all objects at this point into a given collection" in {
     val buffer = mutable.ArrayBuffer.empty[Thing]
     center.start.followedBy.aggregate(buffer).followedBy.iterate
     buffer.toSet shouldBe Set(l1, r1)
  }

  "sort steps should order" in {
    Traversal(1,3,2).sorted.l shouldBe Seq(1,2,3)
    Traversal("aa", "aaa", "a").sortBy(_.length).l shouldBe Seq("a", "aa", "aaa")
  }

  ".help step" should {
    "give a domain overview" in {
      simpleDomain.help should include(".things")
      simpleDomain.help should include("all things")
    }

    "provide node-specific overview" when {
      "using simple domain" in {
        val thingTraversal: Traversal[Thing] = Traversal.empty
        thingTraversal.help should include("Available steps for Thing")
        thingTraversal.help should include(".name")

        thingTraversal.helpVerbose should include("ThingTraversal") // the Traversal classname
        thingTraversal.helpVerbose should include(".sideEffect") // step from Traversal
        thingTraversal.helpVerbose should include(".label") // step from ElementTraversal
        thingTraversal.helpVerbose should include(".out") // step from NodeTraversal
      }

      "using hierarchical domain" in {
        import overflowdb.traversal.testdomains.hierarchical._
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
