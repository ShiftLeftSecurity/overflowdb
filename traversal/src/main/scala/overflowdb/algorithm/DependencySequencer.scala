package overflowdb.algorithm

import scala.collection.mutable

/**
 * TODO doc
 * DAG, topological sort, but in chunks -> nodes that depend on each other etc.
 * e.g. to determine which tasks can run in parallel, and which ones need to run in sequence
 */
object DependencySequencer {

  /** TODO return type: dependency list in order... TODO
   *  */
  def apply[A: GetParents](nodes: Set[A]): Seq[Set[A]] = {
    if (nodes.size == 0) {
      Nil
    } else if (nodes.size == 1) {
      Seq(nodes)
    } else {
      // TODO split up in separate method, call (tail-) recursively
//      val remainder: mutable.Map[A, Set[A]] = nodes
//      val remainder = nodes.to
      val getParents = implicitly[GetParents[A]]
      val leafs = nodes.filter(getParents(_).isEmpty)
      val remainder = nodes.diff(leafs)
      val a = Seq(leafs) ++ DependencySequencer(remainder)
      // TODO need to filter out the nodes that have been worked on already - use a 'visited' helper set
//      ???
      a
    }

  }

//  def leafs(nodes: Set)

}
