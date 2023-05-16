package overflowdb.traversal

import overflowdb.Graph

import scala.jdk.CollectionConverters.IteratorHasAsScala

class InitialTraversal[+A <: overflowdb.Node] private (graph: Graph, label: String, arr: java.util.ArrayList[A])
    extends Iterator[A] {
  private[overflowdb] var idx = 0

  override def hasNext: Boolean = idx < arr.size()

  override def next(): A = {
    if (!hasNext) throw new NoSuchElementException()
    idx = idx + 1
    arr.get(idx - 1)
  }

  // we can only do this if the iterator itself is virgin, e.g. `val trav = cpg.method; trav.next; trav.fullNameExact(...)` cannot use the index
  def canUseIndex(key: String): Boolean = idx == 0 && graph.indexManager.isIndexed(key)

  def getByIndex(key: String, value: Any): Option[Iterator[A]] = {
    if (canUseIndex(key)) {
      val nodes = graph.indexManager.lookup(key, value)
      Some(nodes.iterator().asScala.label(label).cast[A])
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
    new InitialTraversal(graph, label, tmp)
  }
}
