package overflowdb.traversal

import org.scalatest.{Matchers, WordSpec}
import overflowdb._
import overflowdb.traversal._
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

  "choose" should {
//    "provide simple version for if/else semantic" in {
//      graph.V
//        .choose(
//          _.value(Age).is(P.gt(30)),
//          onTrue = _.value(Height),
//          onFalse = _.value(Shoesize)
//        )
//        .toSet shouldBe Set(190,
//        176, // Michael and Steffi are >30 - take their height
//        5) // Karlotta is <=30 - take her shoesize
//    }

    "choose step" should {
      "provide if/elseif semantics" in {
//        simpleDomain.things.has(Name.("[LR]1") //L1, R1

        val chooseTrav = trav.choose(_.property(Name)(
          BranchCase("L1", _.out),
          BranchCase("R1", _.repeat(_.out)(_.times(3))
        )

        chooseTrav.property(Name).toSet shouldBe Set(
          ""
        )
  //        .toSet shouldBe Set(190, // Michael is 34 - take his height
  //        41, //Steffi is 32 - take her shoesize
  //        2015) // Karlotta is case `Otherwise` - take her year of birth
      }
    }
  }

}
