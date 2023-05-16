package overflowdb.traversal

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import overflowdb.traversal.ChainedImplicitsTemp._
import overflowdb.traversal.filter.P
import overflowdb.traversal.testdomains.simple.{ExampleGraphSetup, SimpleDomain, Thing, ThingTraversal}
import overflowdb.traversal.testdomains.gratefuldead._
import overflowdb.{Node, toPropertyKeyOps}

import scala.collection.mutable
import overflowdb.traversal._

import scala.jdk.CollectionConverters.IteratorHasAsScala

class TraversalTests extends AnyWordSpec with ExampleGraphSetup {

  "can only be iterated once" in {
    val one = Iterator.single("one")
    one.size shouldBe 1
    one.size shouldBe 0

    val empty = Iterator.empty[String]
    empty.size shouldBe 0
    empty.size shouldBe 0
  }

  ".sideEffect step should apply provided function and do nothing else" in {
    val sack = mutable.ListBuffer.empty[Node]
    center.start.out.sideEffect(sack.addOne).out.toSetMutable shouldBe Set(l2, r2)
    sack.toSet shouldBe Set(l1, r1)
  }

  ".sideEffectPF step should support PartialFunction and not fail for undefined cases" in {
    val sack = mutable.ListBuffer.empty[Node]

    center.start.out
      .sideEffectPF {
        case node if node.property(Thing.Properties.Name).startsWith("L") =>
          sack.addOne(node)
      }
      .out
      .toSetMutable shouldBe Set(l2, r2)

    sack.toSet shouldBe Set(l1)
  }

  "domain overview" in {
    simpleDomain.all
      .property(Thing.Properties.Name)
      .toSetMutable shouldBe Set("L3", "L2", "L1", "Center", "R1", "R2", "R3", "R4", "R5")
    centerTrav.next().name shouldBe "Center"
    simpleDomain.all.label.toSetMutable shouldBe Set(Thing.Label)
  }

  ".dedup step" should {
    "remove duplicates" in {
      Iterator(1, 2, 1, 3).dedup.l shouldBe List(1, 2, 3)
      Iterator(1, 2, 1, 3).dedupBy(_.hashCode).l shouldBe List(1, 2, 3)
    }

    "allow method only based on hashCode - to ensure the traversal doesn't hold onto elements after they've been consumed" in {
      // when run with -Xmx128m we can hold ~7 of these at a time
      def infiniteTraversalWithLargeElements = new Iterator[Any] {
        var i = 0
        def hasNext = true
        def next(): Any = {
          val arr = Array.ofDim[Long](2048, 1024)
          // assign unique value to make the arrays unique
          arr(i)(i) = i
          i += 1
          arr
        }
      }

      /** using dedup by hash comparison, we can traverse over these elements - already consumed elements are garbage
        * collected
        */
      val traversal = infiniteTraversalWithLargeElements.dedupBy(_.hashCode)
      0.to(128).foreach { i =>
        traversal.next()
      }
    }
  }

  "hasNext check doesn't change contents of traversal" when {
    "path tracking is not enabled" in {
      val trav = centerTrav.followedBy.followedBy
      trav.hasNext shouldBe true
      trav.toSetMutable shouldBe Set(l2, r2)
    }

    "path tracking is enabled" in {
      val trav1 = centerTrav.enablePathTracking.followedBy.followedBy
      val trav2 = centerTrav.enablePathTracking.followedBy.followedBy.path
      trav1.hasNext shouldBe true
      trav2.hasNext shouldBe true
      trav1.toSetMutable shouldBe Set(l2, r2)
      trav2.toSetMutable shouldBe Set(
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

    val traversal: Iterator[Artist] = graph.V().asScala.collectAll[Artist]
    traversal.l shouldBe Seq(artist1, artist2)
  }

  ".union step allows to aggregate multiple steps into one traversal" in {
    def unionTrav = Iterator.single(center).out.union(_.out, _.in)
    unionTrav.toSet shouldBe Set(l2, center, r2)
    unionTrav.l.sortBy(_.property(Thing.Properties.Name)) shouldBe List(center, center, l2, r2)

    // ensure that types are being preserved...
    val verifyIsNodeTraversal: Traversal[Node] = unionTrav
    val verifyIsThingTraversal: Traversal[Thing] = Iterator.single(center).union(_.followedBy, _.followedBy)
  }

  ".sort steps should order" in {
    Iterator(1, 3, 2).sorted shouldBe Seq(1, 2, 3)
    Iterator("aa", "aaa", "a").sortBy(_.length) shouldBe Seq("a", "aa", "aaa")
  }

  "string filter steps" in {
    val graph = SimpleDomain.newGraph
    val Name = Thing.PropertyNames.Name

    graph.addNode(Thing.Label, Name, "regular name")
    graph.addNode(
      Thing.Label,
      Name,
      """multi line name
        |line two
        |""".stripMargin
    )

    graph.V.has(Thing.Properties.Name.where(P.matches(".*"))).size shouldBe 2
    graph.V.has(Thing.Properties.Name.where(P.matches(".*", ".*"))).size shouldBe 2
    SimpleDomain.traversal(graph).things.name(".*").size shouldBe 2
    SimpleDomain.traversal(graph).things.name(".*", ".*").size shouldBe 2
  }

  "number filter steps" in {
    def oneToFour = Iterator(1, 2, 3, 4)
    oneToFour.greaterThanEqual(3).l shouldBe Seq(3, 4)
    oneToFour.greaterThan(3).l shouldBe Seq(4)
    oneToFour.lessThanEqual(3).l shouldBe Seq(1, 2, 3)
    oneToFour.lessThan(3).l shouldBe Seq(1, 2)
    oneToFour.equiv(3).l shouldBe Seq(3)
    oneToFour.between(2, 4).l shouldBe Seq(2, 3)
    oneToFour.inside(1, 4).l shouldBe Seq(2, 3)
    oneToFour.outside(2, 3).l shouldBe Seq(1, 4)
  }

  "`is` filter step" in {
    def oneToFour = Iterator(1, 2, 3, 4)
    oneToFour.is(2).l shouldBe Seq(2)
  }

  "within/without filter steps" in {
    def oneToFour = Iterator(1, 2, 3, 4)
    oneToFour.within(Set(2, 4)).l shouldBe Seq(2, 4)
    oneToFour.without(Set(2, 4)).l shouldBe Seq(1, 3)
  }

  ".help step" should {
    import SimpleDomain._ // for domain specific `DocSearchPackages`

    "give a domain overview" in {
      val helpText = simpleDomain.help
      helpText should include(".things")
      helpText should include("all things")
    }

    "provide node-specific overview" when {
      "using simple domain" in {
        val thingTraversal = SimpleDomain.traversal(SimpleDomain.newGraph).things
        val thingTraversalHelp = thingTraversal.help
        thingTraversalHelp should include("Available steps for Thing")
        thingTraversalHelp should include(".name")
        thingTraversalHelp should include(".name2") // step from helptest.SimpleDomainTraversal

        val thingTraversalHelpVerbose = thingTraversal.helpVerbose
        thingTraversalHelpVerbose should include("ThingTraversal") // the Traversal classname
        thingTraversalHelpVerbose should include(".sideEffect") // step from Traversal
        thingTraversalHelpVerbose should include(".label") // step from ElementTraversal
        thingTraversalHelpVerbose should include(".out") // step from NodeTraversal
        thingTraversalHelpVerbose should include(
          "just like name, but in a different package"
        ) // step from helptest.SimpleDomainTraversal
      }

      "using hierarchical domain" in {
        import overflowdb.traversal.testdomains.hierarchical._
        Iterator.empty[Animal].help should include("species of the animal")
        Iterator.empty[Mammal].help should include("can this mammal swim?")
        Iterator.empty[Elephant].help should include("name of the elephant")
        Iterator.empty[Car].help should include("name of the car")

        // elephant is a mammal (and therefor an animal)
        Iterator.empty[Elephant].canSwim // only verify that it compiles
        Iterator.empty[Elephant].species // only verify that it compiles
        Iterator.empty[Elephant].help should include("species of the animal")
        Iterator.empty[Elephant].help should include("can this mammal swim?")
      }
    }

    "provides generic help" when {
      "using verbose mode" when {
        "traversing nodes" in {
          val thingTraversal: Traversal[Thing] = Iterator.empty
          thingTraversal.helpVerbose should include(".sideEffect")
          thingTraversal.helpVerbose should include(".label")
        }

        "traversing non-nodes" in {
          val stringTraversal = Iterator.empty[String]
          stringTraversal.helpVerbose should include(".sideEffect")
          (stringTraversal.helpVerbose should not).include(".label")
        }
      }
    }
  }

}
