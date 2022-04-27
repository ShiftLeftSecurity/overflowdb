package overflowdb.algorithm

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

class LowestCommonAncestorsTests extends AnyWordSpec {

  /**
   *
   *    +--------------+
   *    |              |
   *    |  +---+     +---+     +---+     +---+     +---+     +---+
   *    |  | A | --> | C | --> | D | --> |   | --> | H | --> | I |
   *    |  +---+     +---+     +---+     |   |     +---+     +---+
   *    |    |         |                 |   |
   *    |    |         +---------------> | G |
   *    |    v                           |   |
   *    |  +---+                         |   |     +---+
   *    |  | B | ----------------------> |   | --> | F |
   *    |  +---+                         +---+     +---+
   *    |    |
   *    |    |
   *    |    v
   *    |  +---+
   *    +> | E |
   *       +---+
   *
   * created by `graph-easy --input=lca.eg`, where lca.eg:
[A] --> [B],[C]
[B] --> [E],[G]
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
  val G = new Node("G", Set(B, C, D))
  val F = new Node("F", Set(G))
  val H = new Node("H", Set(G))
  val I = new Node("I", Set(H))

  "empty set" in {
    val relevantNodes = Set.empty[Node]
    LowestCommonAncestors(relevantNodes)(_.parents) shouldBe Set.empty
  }

  "one node" in {
    val relevantNodes = Set(D)
    LowestCommonAncestors(relevantNodes)(_.parents) shouldBe relevantNodes
  }

  "node E and H" in {
    val relevantNodes = Set(E, H)
    LowestCommonAncestors(relevantNodes)(_.parents) shouldBe Set(B, C)
  }

  "node B,E,H" in {
    val relevantNodes = Set(B, E, H)
    LowestCommonAncestors(relevantNodes)(_.parents) shouldBe Set(A)
  }

  "node A,B,E,H" in {
    val relevantNodes = Set(A, B, E, H)
    LowestCommonAncestors(relevantNodes)(_.parents) shouldBe Set.empty
  }

  "cyclic dependencies" in {
    val A = new Node("A", Set.empty)
    val B = new Node("B", Set(A))
    A.parents = Set(B)  // cycle in dependencies, not a DAG any longer
    LowestCommonAncestors(Set(A, B)) shouldBe Set.empty
  }

  class Node(val name: String, var parents: Set[Node]) {
    override def toString = name
  }
  implicit def getParents: GetParents[Node] = (node: Node) => node.parents
}
