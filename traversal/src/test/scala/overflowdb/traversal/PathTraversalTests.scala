package overflowdb.traversal

import org.scalatest.{Matchers, WordSpec}
import overflowdb._
import overflowdb.traversal.testdomains.simple.Thing.Properties.Name
import overflowdb.traversal.testdomains.simple.{ExampleGraphSetup, Thing}

import scala.collection.mutable


class PathTraversalTests extends WordSpec with Matchers {
  import ExampleGraphSetup._

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

      "dedup" in {
        verifyResults(center.start.enablePathTracking.both.both.dedup.path.toSet)
        verifyResults(center.start.enablePathTracking.both.both.dedupBy(_.hashCode).path.toSet)

        def verifyResults(paths: Set[Seq[_]]) = {
          paths should contain(Seq(center, l1, l2))
          paths should contain(Seq(center, r1, r2))
          //        paths.should(contain(oneOf(Seq(center, l1, center), Seq(center, r1, center))))

          // should container *either* `center, l1, center` *or* `center, r1, center`
          var matchCount = 0
          if (paths.contains(Seq(center, l1, center))) matchCount += 1
          if (paths.contains(Seq(center, r1, center))) matchCount += 1
          matchCount shouldBe 1
        }
      }

      "cast" in {
        val traversal: Traversal[Node] = center.start.enablePathTracking.out.out
        val results: Seq[Thing] = traversal.cast[Thing].l
        results shouldBe Seq(l2, r2)
      }

      "where" in {
        centerTrav.enablePathTracking.followedBy.where(_.nameStartsWith("R")).followedBy.path.toSet shouldBe Set(
          Seq(center, r1, r2))
      }

      "whereNot" in {
        centerTrav.enablePathTracking.followedBy.whereNot(_.nameStartsWith("R")).followedBy.path.toSet shouldBe Set(
          Seq(center, l1, l2))
      }

      "sideEffect" in {
        val sack = mutable.ListBuffer.empty[Node]
        center.start.enablePathTracking.out.sideEffect(sack.addOne).out.path.toSet shouldBe Set(
          Seq(center, l1, l2),
          Seq(center, r1, r2),
        )
        sack.toSet shouldBe Set(l1, r1)
      }

      "sideEffectPF" in {
        val sack = mutable.ListBuffer.empty[Node]

        center
          .start
          .enablePathTracking
          .out
          .sideEffectPF {
            case node if node.property(Thing.Properties.Name).startsWith("L") =>
              sack.addOne(node)
          }
          .out
          .path
          .toSet shouldBe Set(
          Seq(center, l1, l2),
          Seq(center, r1, r2)
        )

        sack.toSet shouldBe Set(l1)
      }

      "or" in {
        centerTrav.enablePathTracking.out.or(
          _.label("does not exist"),
          _.has(Name, "R1")
        ).out.path.l shouldBe Seq(
          Seq(center, r1, r2)
        )
      }

      "and" in {
        centerTrav.enablePathTracking.out.and(
          _.label(Thing.Label),
          _.has(Name, "R1")
        ).out.path.l shouldBe Seq(
          Seq(center, r1, r2)
        )
      }

      "choose" in {
        graph.nodes(Thing.Label).enablePathTracking
          .choose(_.property(Name)) {
            case "L1" => _.out // -> L2
            case "R1" => _.repeat(_.out)(_.times(3)) // -> R4
          }.property(Name).path.toSet shouldBe Set(
          Seq(r1, r4, "R4"),
          Seq(l1, l2, "L2")
        )
      }

      "coalesce" in {
        var traversalInvoked = false
        centerTrav.enablePathTracking.coalesce(
          _.out("doesn't exist"),
          _.out,
          _.sideEffect(_ => traversalInvoked = true).out
        ).property(Name).path.toSet shouldBe Set(
          Seq(center, l1, "L1"),
          Seq(center, r1, "R1"),
        )
        traversalInvoked shouldBe false
      }

    }
  }

  ".simplePath step" should {
    "remove results where path has repeated objects on the path" in {
      center.start.enablePathTracking.both.both.simplePath.toSet shouldBe Set(l2, r2)

      center.start.enablePathTracking.both.both.simplePath.path.toSet shouldBe Set(
        Seq(center, l1, l2),
        Seq(center, r1, r2),
      )
    }

    "throw error if path tracking not enabled" in {
      intercept[AssertionError] { center.start.both.both.simplePath.l }
    }
  }

}
