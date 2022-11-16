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
        center
      ))
    )
  }

  "direct neighbors" in {
    val path = PathFinder(center, r1)
    path shouldBe Seq(
      Path(Seq(
        center, r1
      ))
    )
    path.withEdges shouldBe
  }

}
