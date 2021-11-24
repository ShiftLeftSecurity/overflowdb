package overflowdb.algorithm

import org.scalatest.{Matchers, WordSpec}

class DependencySequencerTests extends WordSpec with Matchers {

  "empty graph" in {
    DependencySequencer(Set.empty[Node]) shouldBe Seq.empty
  }

  "one node" in {
    val A = new Node("A")
    DependencySequencer(Set(A)) shouldBe Seq(Set(A))
  }

  "two independent nodes" in {
    val A = new Node("A")
    val B = new Node("B")
    DependencySequencer(Set(A, B)) shouldBe Seq(Set(A, B))
  }

  "two nodes in sequence" in {
    val A = new Node("A")
    val B = new Node("B", Set(A))
    DependencySequencer(Set(A, B)) shouldBe Seq(Set(A), Set(B))
  }

  "sequence and parallelism - simple 1" in {
    val A = new Node("A")
    val B = new Node("B")
    val C = new Node("C", Set(A, B))
    DependencySequencer(Set(A, B, C)) shouldBe Seq(Set(A, B), Set(C))
  }

  "sequence and parallelism - simple 2" in {
    val A = new Node("A")
    val B = new Node("B", Set(A))
    val C = new Node("C", Set(A))
    DependencySequencer(Set(A, B, C)) shouldBe Seq(Set(A), Set(B, C))
  }

  "throw error if it's not a DAG" in {
    val A = new Node("A")
    val B = new Node("B", Set(A))
    A.parents = Set(B)  // cycle in dependencies, not a DAG any longer
    assertThrows[AssertionError](DependencySequencer(Set(A, B)))
  }

  "larger graph" in {
    /**
     *             +-------------------+
     *             |                   v
     * +---+     +---+     +---+     +---+
     * | A | --> | B | --> | C | --> | E |
     * +---+     +---+     +---+     +---+
     *             |                   ^
     *             v                   |
     *           +---+                 |
     *           | D | ----------------+
     *           +---+
     */
    val A = new Node("A")
    val B = new Node("B", Set(A))
    val C = new Node("C", Set(B))
    val D = new Node("D", Set(B))
    val E = new Node("E", Set(B, C, D))
    DependencySequencer(Set(A, B, C, D, E)) shouldBe Seq(Set(A), Set(B), Set(C, D), Set(E))
  }

  class Node(val name: String, var parents: Set[Node] = Set.empty) {
    override def toString = name
  }
  implicit def getParents: GetParents[Node] = (node: Node) => node.parents
}
