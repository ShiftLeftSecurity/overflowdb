package overflowdb.traversal

import org.scalatest.{Matchers, WordSpec}
import overflowdb._
import overflowdb.traversal._
import overflowdb.traversal.filter.P
import overflowdb.traversal.testdomains.simple.Thing.Properties.Name
import overflowdb.traversal.testdomains.simple.{Connection, ExampleGraphSetup, SimpleDomain, Thing}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class LogicalStepsTests extends WordSpec with Matchers {
  import ExampleGraphSetup._
  /* most tests work with this simple graph:
   * L3 <- L2 <- L1 <- Center -> R1 -> R2 -> R3 -> R4
   */

  "or step returns results if at least one condition is met" in {
    centerTrav.out.or(_.label(Thing.Label)).size shouldBe 2
    centerTrav.out.or(_.label("does not exist")).size shouldBe 0

    centerTrav.out.or(
      _.label("does not exist"),
      _.has(Name, "R1")
    ).size shouldBe 1
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
    ).size shouldBe 1
  }

  "choose step" should {
    "provide if/elseif semantics" in {
      graph.nodes(Thing.Label)
        .choose(_.property(Name)) {
          case "L1" => _.out
          case "R1" => _.repeat(_.out)(_.times(3))
        }.property(Name).toSet shouldBe Set("L2", "R4")
    }

    "provide if/elseif/else semantics" in {
      graph.nodes(Thing.Label)
        .choose(_.property(Name)) {
          case "L1" => _.out
          case _ => _.in
        }.property(Name).toSet shouldBe Set("L2", "L1", "Center", "R1", "R2", "R3")
    }

    "handle empty `on` matching case" in {
      graph.nodes(Thing.Label)
        .choose(_.filter(_ => false)) {
          case _ => _.out
        }.property(Name).size shouldBe 0
    }
  }

}
