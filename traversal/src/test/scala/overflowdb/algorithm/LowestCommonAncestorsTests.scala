package overflowdb.algorithm

import org.scalatest.{Matchers, WordSpec}

class LowestCommonAncestorsTests extends WordSpec with Matchers {

  /**
   *              +-------------------+
   *              |                   v
   *  +---+     +---+     +---+     +---+     +---+     +---+
   *  | A | --> | C | --> | D | --> | G | --> | H | --> | I |
   *  +---+     +---+     +---+     +---+     +---+     +---+
   *    |         |                   |
   *    |         |                   |
   *    v         v                   v
   *  +---+     +---+               +---+
   *  | B |     | E |               | F |
   *  +---+     +---+               +---+
   *
   * created by `graph-easy --input=lca.eg`, where lca.eg:
[A] --> [B],[C]
[C] --> [D],[E],[G]
[D] --> [G]
[G] --> [F],[H]
[H] --> [I]
   *
   */

  val A = new Node("A", Set.empty)
  val B = new Node("B", Set(A))
  val C = new Node("C", Set(A))
  val D = new Node("D", Set(C))
  val E = new Node("E", Set(B, C))
  val G = new Node("F", Set(B, C, D))
  val F = new Node("G", Set(G))
  val H = new Node("H", Set(G))
  val I = new Node("I", Set(H))

  "empty set" in {
    val relevantNodes = Set.empty[Node]
    LowestCommonAncestors(relevantNodes) shouldBe Set.empty
  }

  "one node" in {
    val relevantNodes = Set(D)
    LowestCommonAncestors(relevantNodes) shouldBe relevantNodes
  }

  "node E and H" in {
    val relevantNodes = Set(E, H)
    LowestCommonAncestors(relevantNodes) shouldBe Set(B, C)
  }

  "node B,E,H" in {
    val relevantNodes = Set(B, E, H)
    LowestCommonAncestors(relevantNodes) shouldBe Set(A)
  }

  "node A,B,E,H" in {
    val relevantNodes = Set(A, B, E, H)
    LowestCommonAncestors(relevantNodes) shouldBe Set.empty
  }

  class Node(val name: String, val parents: Set[Node]) {
    override def toString = name
  }
  implicit def getParents: GetParents[Node] = (node: Node) => node.parents
}
