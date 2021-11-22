package overflowdb.algorithm

/**
 * TODO doc
 * DAG, topological sort -> nodes that depend on each other etc.
 * e.g. to determine which tasks can run in parallel, and which ones need to run in sequence
 */
object DependencySequencer {


  /** TODO return type: dependency list in order... TODO
   *  */
  def apply[A: GetParents](nodes: Set[A]): Seq[Set[A]] = {
    def parentsRecursive(node: A): Set[A] = {
      val nodeParents = implicitly[GetParents[A]].apply(node)
      nodeParents ++ nodeParents.flatMap(parentsRecursive)
    }

    if (nodes.size == 0) {
      Nil
    } else if (nodes.size == 1) {
      Seq(nodes)
    } else {
//      val (head, tail) = (nodes.head, nodes.tail)
//      val parentsIntersection = tail.foldLeft(parentsRecursive(head)) {
//        case (res, next) =>
//          res.intersect(parentsRecursive(next))
//      }
//
//      parentsIntersection.filter { node =>
//        val childCount = parentsIntersection.count(parentsRecursive(_).contains(node))
//        childCount == 0
//      }
      ???
    }
  }

  trait GetParents[A] {
    def apply(a: A): Set[A]
  }
}
