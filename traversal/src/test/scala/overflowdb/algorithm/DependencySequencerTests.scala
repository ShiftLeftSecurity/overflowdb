package overflowdb.algorithm

import org.scalatest.{Matchers, WordSpec}

class DependencySequencerTests extends WordSpec with Matchers {

  "empty graph" in {
    DependencySequencer(Set.empty[Node]) shouldBe Seq.empty
  }

  "one node" in {
    val _0 = new Node(0)
    DependencySequencer(Set(_0)) shouldBe Seq(Set(_0))
  }

  "two independent nodes" in {
    val _0 = new Node(0)
    val _1 = new Node(1)
    DependencySequencer(Set(_0, _1)) shouldBe Seq(Set(_0, _1))
  }

  "two nodes in sequence" in {
    val _0 = new Node(0)
    val _1 = new Node(1, Set(_0))
    DependencySequencer(Set(_0, _1)) shouldBe Seq(Set(_0), Set(_1))
  }

  "sequence and parallelism - simple 1" in {
    val _0 = new Node(0)
    val _1 = new Node(1)
    val _2 = new Node(2, Set(_0, _1))
    DependencySequencer(Set(_0, _1, _2)) shouldBe Seq(Set(_0, _1), Set(_2))
  }

  "sequence and parallelism - simple 2" in {
    val _0 = new Node(0)
    val _1 = new Node(1, Set(_0))
    val _2 = new Node(2, Set(_0))
    DependencySequencer(Set(_0, _1, _2)) shouldBe Seq(Set(_0), Set(_1, _2))
  }

  "throw error if it's not a DAG" in {
    val _0 = new Node(0)
    val _1 = new Node(1, Set(_0))
    _0.parents = Set(_1)  // cycle in dependencies, not a DAG any longer
    assertThrows[AssertionError](DependencySequencer(Set(_0, _1)))
  }

  "larger graph" in {

    /**
     *             +-------------------+
     *             |                   v
     * +---+     +---+     +---+     +---+
     * | 0 | --> | 1 | --> | 2 | --> | 4 |
     * +---+     +---+     +---+     +---+
     *             |                   ^
     *             v                   |
     *           +---+                 |
     *           | 3 | ----------------+
     *           +---+
     */
    val _0 = new Node(0)
    val _1 = new Node(1, Set(_0))
    val _2 = new Node(2, Set(_1))
    val _3 = new Node(3, Set(_1))
    val _4 = new Node(4, Set(_1, _2, _3))
    DependencySequencer(Set(_0, _1, _2, _3, _4)) shouldBe Seq(Set(_0), Set(_1), Set(_2, _3), Set(_4))
  }

  class Node(val value: Int, var parents: Set[Node] = Set.empty) {
    override def toString = s"Node($value)"
  }
  implicit def getParents: GetParents[Node] = (node: Node) => node.parents
}
