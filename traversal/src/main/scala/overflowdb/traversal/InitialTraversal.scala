package overflowdb.traversal

import overflowdb.Graph

class InitialTraversal[+A <: overflowdb.Node] private (graph: Graph,
                                                       label: String,
                                                       iter: ArrayListIter[A])
    extends Traversal[A](iter) {

  def getByIndex(key: String, value: Any): Traversal[A] = {
    // we can only do this if the iterator itself is virgin, e.g. `val trav = cpg.method; trav.next; trav.fullNameExact(...)` cannot use the index
    if (iter.idx == 0 && graph.indexManager.isIndexed(key)) {
      val nodes = graph.indexManager.lookup(key, value)
      Traversal.from(nodes.iterator()).cast[A]
    } else {
      null
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
private[overflowdb] class ArrayListIter[+T](arr: java.util.ArrayList[T]) extends Iterator[T] {
  private[overflowdb] var idx = 0

  override def hasNext: Boolean = idx < arr.size()

  override def next(): T = {
    if (!hasNext) throw new NoSuchElementException()
    idx = idx + 1
    arr.get(idx - 1)
  }
}
