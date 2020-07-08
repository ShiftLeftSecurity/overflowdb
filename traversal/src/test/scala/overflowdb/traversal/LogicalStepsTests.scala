package overflowdb.traversal

import org.scalatest.{Matchers, WordSpec}
import overflowdb._
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
    centerTrav.out.or(_.label("does not exist")).size shouldBe 0

    centerTrav.out.or(
      _.label("does not exist"),
//      _.property(Name, "R1")
    ).size shouldBe 1
  }


}
