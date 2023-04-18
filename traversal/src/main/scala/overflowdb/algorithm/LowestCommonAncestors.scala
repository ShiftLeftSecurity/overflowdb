package overflowdb.algorithm

import scala.annotation.tailrec

object LowestCommonAncestors {

  /** Find the lowest common ancestor(s) for a set of nodes in a directed acyclic graph (DAG).
    * @return
    *   Set.empty if given nodes have cyclic dependencies
    *
    * Algorithm: 1) for each relevant node, find their recursive parents 2) create the intersection of all of those sets
    * 3) the LCA are those nodes, that do not have any children in that set
    *
    * based on https://www.baeldung.com/cs/lowest-common-ancestor-acyclic-graph
    */
  def apply[A: GetParents](nodes: Set[A]): Set[A] = {
    if (nodes.size <= 1) {
      nodes
    } else {
      val (head, tail) = (nodes.head, nodes.tail)
      val parentsIntersection = tail.foldLeft(parentsRecursive(head)) { case (res, next) =>
        res.intersect(parentsRecursive(next))
      }

      parentsIntersection.filter { node =>
        val childCount = parentsIntersection.count(parentsRecursive(_).contains(node))
        childCount == 0
      }
    }
  }

  def parentsRecursive[A: GetParents](node: A): Set[A] =
    parentsRecursive0(Set(node), Set.empty, Set.empty)

  @tailrec
  private def parentsRecursive0[A: GetParents](nodes: Set[A], accumulator: Set[A], visited: Set[A]): Set[A] = {
    if (nodes.isEmpty || nodes.forall(visited.contains))
      accumulator
    else {
      val getParents = implicitly[GetParents[A]]
      val parents = nodes.flatMap(getParents.apply)
      val nextAccumulator = accumulator ++ parents

      parentsRecursive0(parents, nextAccumulator, visited ++ nodes)
    }

  }

}
