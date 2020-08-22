package overflowdb.traversal

import org.scalatest.{Matchers, WordSpec}
import overflowdb.Node
import overflowdb.traversal.testdomains.simple.Thing.Properties.Name
import overflowdb.traversal.testdomains.simple.{ExampleGraphSetup, Thing}

class LogicalStepsTests extends WordSpec with Matchers {
  import ExampleGraphSetup._
  /* most tests work with this simple graph:
   * L3 <- L2 <- L1 <- Center -> R1 -> R2 -> R3 -> R4 -> R5
   */

  "or step returns results if at least one condition is met" in {
    centerTrav.out.or(_.label(Thing.Label)).size shouldBe 2
    centerTrav.out.or(_.label("does not exist")).size shouldBe 0

    centerTrav.out.or(
      _.label("does not exist"),
      _.has(Name, "R1")
    ).l shouldBe Seq(r1)
  }

  "and step returns results if ALL conditions are met" in {
    centerTrav.out.and(_.label(Thing.Label)).size shouldBe 2
    centerTrav.out.and(_.label("does not exist")).size shouldBe 0

    centerTrav.out.and(
      _.label("does not exist"),
      _.has(Name, "R1")
    ).size shouldBe 0

    centerTrav.out.and(
      _.label(Thing.Label),
      _.has(Name, "R1")
    ).l shouldBe Seq(r1)
  }

  "choose step" should {
    "provide if semantics" in {
      graph.nodes(Thing.Label)
        .choose(_.property(Name)) {
          case "L1" => _.out // -> L2
        }.property(Name).toSet shouldBe Set("L2")
    }

    "provide if/elseif semantics" in {
      graph.nodes(Thing.Label)
        .choose(_.property(Name)) {
          case "L1" => _.out // -> L2
          case "R1" => _.repeat(_.out)(_.times(3)) // -> R4
        }.property(Name).toSet shouldBe Set("L2", "R4")
    }

    "provide if/else semantics" in {
      graph.nodes(Thing.Label)
        .choose(_.property(Name)) {
          case "L1" => _.out // -> L2
          case "R1" => _.repeat(_.out)(_.times(3)) // -> R4
          case _ => _.in
        }.property(Name).toSet shouldBe Set("L2", "L1", "R1", "R2", "R3", "R4")
    }

    "handle empty `on` traversal: if semantics" in {
      graph.nodes(Thing.Label)
        .choose(_.property(Name).filter(_ => false)) {
          case "L1" => _.out
        }.property(Name).size shouldBe 0
    }

    "handle empty `on` traversal: if/else semantics" in {
      graph.nodes(Thing.Label)
        .choose(_.property(Name).filter(_ => false)) {
          case "L1" => _.in
          case _ => _.out
        }.property(Name).toSet shouldBe Set("L3", "L2", "L1", "R1", "R2", "R3", "R4", "R5")
    }
  }

  "coalesce step takes arbitrary number of traversals and follows the first one that returns at least one element" in {
    centerTrav.coalesce(_.out).property(Name).toSet shouldBe Set("L1", "R1")

    centerTrav.coalesce().size shouldBe 0
    centerTrav.coalesce(_.out("doesn't exist")).size shouldBe 0

    // verify it doesn't invoke the third traversal
    var thirdTraversalInvoked = false
    centerTrav.coalesce(
      _.out("doesn't exist"),
      _.out,
      _.sideEffect(_ => thirdTraversalInvoked = true).out
    ).property(Name).toSet shouldBe Set("L1", "R1")
    thirdTraversalInvoked shouldBe false

    centerTrav.coalesce(
      _.name("doesn't exist"),
      _.followedBy
    ).name.toSet shouldBe Set("L1", "R1")

    // we can even mix generic graph steps (.out) and domain-specific steps (.followedBy), but need to help the type
    // inferencer by specifying `[Node]` as the result type
    centerTrav.coalesce[Node](_.out, _.followedBy).property(Name).toSet shouldBe Set("L1", "R1")
  }

}
