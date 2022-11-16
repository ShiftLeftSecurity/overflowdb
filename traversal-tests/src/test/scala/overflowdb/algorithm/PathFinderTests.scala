package overflowdb.algorithm

import overflowdb.traversal.testdomains.simple.{Connection, ExampleGraphSetup}
import PathFinder._
import overflowdb.Direction._
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class PathFinderTests extends AnyWordSpec {
  import ExampleGraphSetup._
  /* most tests work with this simple graph:
   * L3 <- L2 <- L1 <- Center -> R1 -> R2 -> R3 -> R4 -> R5
   */

  "identity" in {
    PathFinder(center, center) shouldBe Seq(
      Path(Seq(
        NodeEntry(center)
      ))
    )
  }

  "direct neighbors" in {
    PathFinder(center, r1) shouldBe Seq(
      Path(Seq(
        NodeEntry(center), EdgeEntry(OUT, Connection.Label), NodeEntry(r1)
      ))
    )
  }

}
