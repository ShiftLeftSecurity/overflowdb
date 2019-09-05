package io.shiftleft.overflowdb.traversals

import io.shiftleft.overflowdb.{NodeRef, OdbGraph}
import org.apache.tinkerpop.gremlin.structure.Vertex

import scala.collection.immutable.ArraySeq
import scala.collection.{Iterable, IterableFactory, IterableFactoryDefaults, IterableOnce, IterableOps, Iterator, mutable}
import scala.jdk.CollectionConverters._

class Traversal[+A](elements: IterableOnce[A]) extends Iterable[A]
  with IterableOps[A, Traversal, Traversal[A]]
  with IterableFactoryDefaults[A, Traversal] {
  override def className = "Traversal"
  override def iterableFactory: IterableFactory[Traversal] = Traversal
  override def iterator: Iterator[A] = elements.iterator

  def l: ArraySeq[A] = elements.iterator.to(ArraySeq.untagged)
}

object Traversal extends IterableFactory[Traversal] {
  private[this] val _empty = new Traversal(Iterator.empty)

  def empty[A]: Traversal[A] = _empty

  def newBuilder[A]: mutable.Builder[A, Traversal[A]] =
    Iterator.newBuilder[A].mapResult(new Traversal(_))

  def from[A](source: IterableOnce[A]): Traversal[A] =
    new Traversal(Iterator.from(source))
}

abstract class TraversalSource(graph: OdbGraph) {
  def all: Traversal[Vertex] = new Traversal(graph.vertices().asScala)

  protected def nodesByLabel(label: String): Traversal[NodeRef[_]] =
    new Traversal(graph.nodesByLabel(label).asScala)

  protected def nodesByLabelTyped[A <: NodeRef[_]](label: String): Traversal[A] =
    nodesByLabel(label).map(_.asInstanceOf[A])
}

