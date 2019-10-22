package io.shiftleft.overflowdb.traversals

import scala.collection.immutable.{ArraySeq, IndexedSeq}
import scala.collection.{Iterable, IterableFactory, IterableFactoryDefaults, IterableOnce, IterableOps, Iterator, mutable}
import scala.jdk.CollectionConverters._

class Traversal[+A](elements: IterableOnce[A]) extends Iterable[A]
  with IterableOps[A, Traversal, Traversal[A]]
  with IterableFactoryDefaults[A, Traversal] {
  def this(elements: java.util.Iterator[A]) = this(elements.asScala)

  def l: IndexedSeq[A] = elements.iterator.to(ArraySeq.untagged)
  def cast[B]: Traversal[B] = new Traversal[B](elements.iterator.map(_.asInstanceOf[B]))

  override def className = "Traversal"
  override def iterableFactory: IterableFactory[Traversal] = Traversal
  override def iterator: Iterator[A] = elements.iterator
}

object Traversal extends IterableFactory[Traversal] {
  private[this] val _empty = new Traversal(Iterator.empty)
  def empty[A]: Traversal[A] = _empty

  def newBuilder[A]: mutable.Builder[A, Traversal[A]] = Iterator.newBuilder[A].mapResult(new Traversal(_))
  def from[A](source: IterableOnce[A]): Traversal[A] = new Traversal(Iterator.from(source))
  def fromSingle[A](a: A): Traversal[A] = new Traversal(Iterator.single(a))
}
