package overflowdb.traversal

import org.scalatest.{Matchers, WordSpec}
import overflowdb.traversal.testdomains.simple.Thing.Properties.Name
import overflowdb.traversal.testdomains.simple.{ExampleGraphSetup, Thing}

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

    "provide if/else semantics" in {
      graph.nodes(Thing.Label)
        .choose(_.property(Name)) {
          case "L1" => _.out // will traverse to L2
          case _ => _.in
        }.property(Name).toSet shouldBe Set("L2", "L1", "Center", "R1", "R2", "R3")
    }

    "handle empty `on` matching case" in {
      graph.nodes(Thing.Label)
        .choose(_.has("nonExistingProperty")) {
          case node => _.out
        }.property(Name).toSet shouldBe Set("L3", "L2", "L1", "R1", "R2", "R3", "R4")
    }

    "foo" in {
      import overflowdb.Node
      val b = new ChooseBehaviour.Builder[Node, String, Node]
      b.withBranch { case "asd" => _.out }

      graph.nodes(Thing.Label)
        .choose2(_.has("nonExistingProperty")){ b: ChooseBehaviour.Builder[Node, String, Node] =>
//          _.withBranch { case "L1" => _.out }
//          _.withBranch[Node] { case "L1" => {x: Traversal[Node] => x.out }}
            b.withBranch[Node] {
              val pf: PartialFunction[String, Traversal[Node] => Traversal[Node]] = ???
              pf
            }
//        _.withBranch { ??? }
//           .otherwise(_.in)
        }
        //.property(Name).toSet shouldBe Set("L3", "L2", "L1", "R1", "R2", "R3", "R4")
//      graph.nodes(Thing.Label)
////        .choose(_.has("name")) {
//        .choose(_.property(Name)) {
//          case "L2" => _.out
//          case _ => _.in
//        }

    }
  }

}
