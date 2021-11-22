package overflowdb.algorithm

import org.scalatest.{Matchers, WordSpec}
import overflowdb.algorithm.DependencySequencer.GetParents

class DependencySequencerTests extends WordSpec with Matchers {
  class Node(val value: Int, val parents: Set[Node]) {
    override def toString = s"Node($value)"
  }
  implicit val getParents = new GetParents[Node] {
    override def apply(node: Node): Set[Node] = node.parents
  }

  "empty graph" in {
    DependencySequencer(Set.empty[Node]) shouldBe Seq.empty
  }

  "one node" in {
    val _0 = new Node(0, Set.empty)
    DependencySequencer(Set(_0)) shouldBe Seq(Set(_0))
  }

  "two independent nodes" in {
    val _0 = new Node(0, Set.empty)
    val _1 = new Node(1, Set.empty)
    DependencySequencer(Set(_0, _1)) shouldBe Seq(Set(_0, _1))
  }

  "two nodes in sequence" in {
    val _0 = new Node(0, Set.empty)
    val _1 = new Node(1, Set(_0))
    DependencySequencer(Set(_0, _1)) shouldBe Seq(Set(_0), Set(_1))
  }

}
