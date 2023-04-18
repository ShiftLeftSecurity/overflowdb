package overflowdb.algorithm

import scala.annotation.tailrec

object DependencySequencer {

  /** Find the sequence of dependencies a set of nodes in a directed acyclic graph (DAG). Sample use case: concurrent
    * task processing: given a set of tasks, determine which ones can be executed in parallel, and which ones need to
    * run in sequence.
    *
    * @throws java.lang.AssertionError
    *   if given nodes have cyclic dependencies
    *
    * Algorithm: variant of Kahn's algorithm for topological sort 1) for given nodes, find all leaves, i.e. the those
    * without parents (e.g. task dependencies) 2) disregard all that have already been visited and add to the results
    * sequence 3) repeat for the remainder of nodes
    *
    * see https://en.wikipedia.org/wiki/Topological_sorting#Kahn%27s_algorithm
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
      assert(
        remainder.size < nodes.size,
        s"given set of nodes is not a directed acyclic graph (DAG): ${nodes ++ accumulator.flatten}"
      )
      apply0(remainder, accumulator :+ leaves, visited ++ leaves)
    }
  }

}
