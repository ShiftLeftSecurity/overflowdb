package io.shiftleft.overflowdb.traversals

import org.scalatest.{Matchers, WordSpec}

class TraversalTests extends WordSpec with Matchers {

  "can only be iterated once" in {
    val one = Traversal.fromSingle("one")
    one.size shouldBe 1
    one.size shouldBe 0 // logs a warning (not tested here)

    val empty = Traversal(Nil)
    empty.size shouldBe 0
    empty.size shouldBe 0 // logs a warning (not tested here)
  }

}
