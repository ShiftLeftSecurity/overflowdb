package overflowdb.algorithm

import scala.annotation.tailrec

object DependencySequencer {

  /**
   * For a given set of nodes, TODO
   * * fails if it's not a DAG
   * DAG, topological sort, but in chunks -> nodes that depend on each other etc.
   * e.g. to determine which tasks can run in parallel, and which ones need to run in sequence
   * - variant of kahn's algorithm for topological sort
   *
   * TODO return type: dependency list in order... TODO
   */
  def apply[A: GetParents](nodes: Set[A]): Seq[Set[A]] = {
    apply0(nodes, Seq.empty, Set.empty)
  }

  @tailrec
  private def apply0[A: GetParents](nodes: Set[A], accumulator: Seq[Set[A]], visited: Set[A]): Seq[Set[A]] = {
    if (nodes.size == 0) {
      accumulator
    } else {
      val getParents = implicitly[GetParents[A]]
      val leaves = nodes.filter(getParents(_).diff(visited).isEmpty)
      val remainder = nodes.diff(leaves)
      assert(remainder.size < nodes.size, s"given set of nodes is not a directed acyclic graph (DAG) $nodes")
      apply0(remainder, accumulator :+ leaves, visited ++ leaves)
    }

  }

}
