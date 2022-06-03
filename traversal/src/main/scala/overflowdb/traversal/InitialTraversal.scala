package overflowdb.traversal

import overflowdb.Graph

class InitialTraversal[+A <: overflowdb.Node] private (graph: Graph,
                                                       label: String,
                                                       iter: ArrayListIter[A])
    extends Traversal[A](iter) {

  def getByIndex(key: String, value: Any): Traversal[A] = {
    if (iter.idx == 0 && graph.indexManager.isIndexed(key)) {
      val nodes = graph.indexManager.lookup(key, value)
      Traversal.from(nodes.iterator()).asInstanceOf[Traversal[A]]
    } else {
      null
    }
  }
}

object InitialTraversal {
  def from[A](graph: Graph, label: String): InitialTraversal[A] = {
    new InitialTraversal(
      graph,
      label,
      new ArrayListIter[A](graph.nodes.nodesByLabel(label).asInstanceOf[java.util.ArrayList[A]]))
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
