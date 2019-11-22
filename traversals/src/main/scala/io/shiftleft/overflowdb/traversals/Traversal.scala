package io.shiftleft.overflowdb.traversals

import org.slf4j.LoggerFactory

import scala.collection.immutable.{ArraySeq, IndexedSeq}
import scala.collection.{Iterable, IterableFactory, IterableFactoryDefaults, IterableOps, Iterator, mutable}
import scala.jdk.CollectionConverters._

/**
 * TODO more docs
 *
 * Just like Tinkerpop3 and most other Iterators, a Traversal can only be executed once.
 * Since this may trip up users, we'll log a warning
 * */
class Traversal[+A](elements: IterableOnce[A]) extends Iterable[A]
  with IterableOps[A, Traversal, Traversal[A]]
  with IterableFactoryDefaults[A, Traversal] {

  def l: IndexedSeq[A] = elements.iterator.to(ArraySeq.untagged)
  def cast[B]: Traversal[B] = new Traversal[B](elements.iterator.map(_.asInstanceOf[B]))

  override def className = getClass.getSimpleName
  override def toString = className
  override def iterableFactory: IterableFactory[Traversal] = Traversal

  override val iterator: Iterator[A] = new Iterator[A] {
    private val wrappedIter = elements.iterator
    private var isExhausted = false

    override def hasNext: Boolean = {
      val _hasNext = wrappedIter.hasNext
      if (!_hasNext) {
        if (isExhausted) Traversal.logger.warn("warning: Traversal already exhausted")
        else isExhausted = true
      }
      _hasNext
    }
    override def next(): A = wrappedIter.next
  }
}

object Traversal extends IterableFactory[Traversal] {
  protected val logger = LoggerFactory.getLogger("Traversal")
  private[this] val _empty = new Traversal(Iterator.empty)
  def empty[A]: Traversal[A] = _empty

  def apply[A](elements: IterableOnce[A]) = new Traversal[A](elements.iterator)
  def apply[A](elements: java.util.Iterator[A]) = new Traversal[A](elements.asScala)
  def newBuilder[A]: mutable.Builder[A, Traversal[A]] = Iterator.newBuilder[A].mapResult(new Traversal(_))
  def from[A](source: IterableOnce[A]): Traversal[A] = new Traversal(Iterator.from(source))
  def fromSingle[A](a: A): Traversal[A] = new Traversal(Iterator.single(a))
}