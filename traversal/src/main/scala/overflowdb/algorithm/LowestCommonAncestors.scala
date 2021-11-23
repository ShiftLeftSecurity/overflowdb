package overflowdb.algorithm

/**
  * Find the lowest common ancestor(s)
  *
  * 1) for each relevant node, find their recursive parents
  * 2) create the intersection of all of those sets
  * 3) the LCA are those nodes, that do not have any children in that set
  *
  * based on https://www.baeldung.com/cs/lowest-common-ancestor-acyclic-graph
  */
object LowestCommonAncestors {

  def apply[A: GetParents](nodes: Set[A]): Set[A] = {
    def parentsRecursive(node: A): Set[A] = {
      val nodeParents = implicitly[GetParents[A]].apply(node)
      nodeParents ++ nodeParents.flatMap(parentsRecursive)
    }

    if (nodes.size <= 1) {
      nodes
    } else {
      val (head, tail) = (nodes.head, nodes.tail)
      val parentsIntersection = tail.foldLeft(parentsRecursive(head)) {
        case (res, next) =>
          res.intersect(parentsRecursive(next))
      }

      parentsIntersection.filter { node =>
        val childCount = parentsIntersection.count(parentsRecursive(_).contains(node))
        childCount == 0
      }
    }
  }

}
