package overflowdb.traversal

import overflowdb.Graph

class InitialTraversal[+A <: overflowdb.Node] private (graph: Graph,
                                                       label: String,
                                                       iter: ArrayListIter[A])
    extends Traversal[A](iter) {

  // we can only do this if the iterator itself is virgin, e.g. `val trav = cpg.method; trav.next; trav.fullNameExact(...)` cannot use the index
  def canUseIndex(key: String): Boolean = iter.idx == 0 && graph.indexManager.isIndexed(key)

  def getByIndex(key: String, value: Any): Option[Traversal[A]] = {
    if (canUseIndex(key)) {
      val nodes = graph.indexManager.lookup(key, value)
      Some(Traversal.from(nodes.iterator()).label(label).cast[A])
    } else {
      None
    }
  }
}

object InitialTraversal {
  def from[A <: overflowdb.Node](graph: Graph, label: String): InitialTraversal[A] = {
    val tmp = overflowdb.Misc
      .extractNodesList(graph)
      .nodesByLabel(label)
      .asInstanceOf[java.util.ArrayList[A]]
    val tmp2 = new ArrayListIter(tmp)
    new InitialTraversal(graph, label, tmp2)
  }
}

//This is almost equivalent to the standard ArrayList iterator, except that:
//  1. We can access idx in order to check whether the iterator is still virgin
//  2. We don't attempt to check for concurrent modifications
//  3. It is a scala iterator instead of a java iterator, so we don't need to wrap it
private[overflowdb] class ArrayListIter[+T](arr: java.util.ArrayList[T]) extends Iterator[T] {
  private[overflowdb] var idx = 0

  override def hasNext: Boolean = idx < arr.size()

  override def next(): T = {
    if (!hasNext) throw new NoSuchElementException()
    idx = idx + 1
    arr.get(idx - 1)
  }
}
