package overflowdb.algorithm

import org.scalatest.{Matchers, WordSpec}

class LowestCommonAncestorsTests extends WordSpec with Matchers {
  class Node(val value: Int, val parents: Set[Node]) {
    override def toString = s"Node($value)"
  }

  val _0 = new Node(0, Set.empty)
  val _1 = new Node(1, Set(_0))
  val _2 = new Node(2, Set(_0))
  val _3 = new Node(3, Set(_2))
  val _4 = new Node(4, Set(_1, _2))
  val _6 = new Node(6, Set(_1, _2, _3))
  val _5 = new Node(5, Set(_6))
  val _7 = new Node(7, Set(_6))
  val _8 = new Node(8, Set(_7))

  "empty set" in {
    val relevantNodes = Set.empty[Node]
    LowestCommonAncestors(relevantNodes)(_.parents) shouldBe Set.empty
  }

  "one node" in {
    val relevantNodes = Set(_3)
    LowestCommonAncestors(relevantNodes)(_.parents) shouldBe relevantNodes
  }

  "node 4 and 7" in {
    val relevantNodes = Set(_4, _7)
    LowestCommonAncestors(relevantNodes)(_.parents) shouldBe Set(_1, _2)
  }

  "node 1,4,7" in {
    val relevantNodes = Set(_1, _4, _7)
    LowestCommonAncestors(relevantNodes)(_.parents) shouldBe Set(_0)
  }

  "node 0,1,4,7" in {
    val relevantNodes = Set(_0, _1, _4, _7)
    LowestCommonAncestors(relevantNodes)(_.parents) shouldBe Set.empty
  }

}
