package overflowdb.traversal

import org.scalatest.{Matchers, WordSpec}
import overflowdb.traversal.testdomains.simple.ExampleGraphSetup.centerTrav
import overflowdb.traversal.testdomains.simple.{Connection, ExampleGraphSetup, SimpleDomain, Thing}

import scala.collection.mutable

object Foo extends App {
  import overflowdb._

  val graph = SimpleDomain.newGraph
  def addThing(name: String) = graph + (Thing.Label, Thing.Properties.Name -> name)
  val center = addThing("Center")
  val l1 = addThing("L1")
  val r1 = addThing("R1")
  val l2 = addThing("L2")
  val r2 = addThing("R2")
  val l3 = addThing("L3")
  val r3 = addThing("R3")
  val r4 = addThing("R4")
  center --- Connection.Label --> l1
  l1 --- Connection.Label --> l2
  l2 --- Connection.Label --> l3
  center --- Connection.Label --> r1
  r1 --- (Connection.Label, Connection.Properties.Distance -> 10) --> r2
  r2 --- (Connection.Label, Connection.Properties.Distance -> 10) --> r3
  r3 --- (Connection.Label, Connection.Properties.Distance -> 13) --> r4

  def centerTrav = Traversal.fromSingle(center)
//  centerTrav.path.foreach(println)
//  centerTrav.out3.path.foreach(n => println(s"result: $n"))

  centerTrav.out3.out3.foreach(n => println(s"result: $n"))
//  centerTrav.out3.out3.path.foreach(n => println(s"result: $n"))

//  centerTrav.out3.out3.out3.path.foreach(println)
//  centerTrav.out3.out3.out3.out3.path.foreach(println)
}

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
    simpleDomain.all.property(Thing.Properties.Name).toSet shouldBe Set("L3", "L2", "L1", "Center", "R1", "R2", "R3", "R4")
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
    "work for single entry traversal" in {
      val paths = centerTrav.path.toSet
      names(paths) shouldBe Set(
        List("Center")
      )
    }

    "work for simple one-step expansion" in {
      val paths = centerTrav.out3.path.toSet
      names(paths) shouldBe Set(
        List("Center", "L1"),
        List("Center", "R1")
      )
    }

    "work for two-step expansion" in {
      val paths = centerTrav.out3.out3.path.toSet
      names(paths) shouldBe Set(
        List("Center", "L1", "L2"),
        List("Center", "R1", "R2")
      )
    }

    def names(paths: Set[Seq[Any]]): Set[Seq[String]] =
       paths.map(_.map(_.asInstanceOf[Thing].name))
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
