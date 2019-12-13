package io.shiftleft.overflowdb.traversal

import io.shiftleft.overflowdb.traversal
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.immutable.{ArraySeq, IndexedSeq}
import scala.collection.{
  mutable,
  Iterable,
  IterableFactory,
  IterableFactoryDefaults,
  IterableOnce,
  IterableOps,
  Iterator
}
import scala.jdk.CollectionConverters._

/**
  * TODO more docs
  *
  * Just like Tinkerpop3 and most other Iterators, a Traversal can only be executed once.
  * Since this may trip up users, we'll log a warning
 **/
class Traversal[A](elements: IterableOnce[A])
    extends IterableOnce[A]
    with IterableOps[A, Traversal, Traversal[A]]
    with IterableFactoryDefaults[A, Traversal] {

  def l: IndexedSeq[A] = elements.iterator.to(ArraySeq.untagged)

  def cast[B]: Traversal[B] =
    new Traversal[B](elements.iterator.map(_.asInstanceOf[B]))

  /** perform side effect without changing the contents of the traversal */
  def sideEffect(fun: A => Unit): Traversal[A] = map { a =>
    fun(a)
    a
  }

  def repeat(repeatTraversal: Traversal[A] => Traversal[A],
             behaviourBuilder: RepeatBehaviour.Builder[A] => RepeatBehaviour.Builder[A] = identity)
    : Traversal[A] = {
    val behaviour = behaviourBuilder(new traversal.RepeatBehaviour.Builder[A]).build
    _repeat(repeatTraversal, behaviour)
  }

  @tailrec
  private def _repeat(repeatTraversal: Traversal[A] => Traversal[A],
                      behaviour: RepeatBehaviour[A],
                      emitSack: mutable.ListBuffer[A] = mutable.ListBuffer.empty): Traversal[A] = {
    if (this.isEmpty) {
      // we're at the end - emit whatever we collected on the way
      emitSack.to(Traversal)
    } else {
      val mainTraversal = behaviour match {
        case _: EmitNothing[A]    => this
        case _: EmitEverything[A] => this.sideEffect(emitSack.addOne(_))
        case emitConditional =>
          this.sideEffect { a =>
            if (emitConditional.emit(a)) emitSack.addOne(a)
          }
      }
      repeatTraversal(mainTraversal)._repeat(repeatTraversal, behaviour, emitSack)
    }
  }

  override val iterator: Iterator[A] = new Iterator[A] {
    private val wrappedIter = elements.iterator
    private var isExhausted = false

    override def hasNext: Boolean = {
      val _hasNext = wrappedIter.hasNext
      if (!_hasNext) {
        if (isExhausted)
          Traversal.logger.warn("warning: Traversal already exhausted")
        else isExhausted = true
      }
      _hasNext
    }

    override def next(): A = wrappedIter.next
  }

  override def toString = getClass.getSimpleName

  override def iterableFactory: IterableFactory[Traversal] = Traversal

  override def toIterable: Iterable[A] = Iterable.from(elements)

  override protected def coll: Traversal[A] = this
}

object Traversal extends IterableFactory[Traversal] {
  protected val logger = LoggerFactory.getLogger("Traversal")

  //  private[this] val _empty = new Traversal(Iterator.empty)
  def empty[A]: Traversal[A] = new Traversal(Iterator.empty)

  def apply[A](elements: IterableOnce[A]) = new Traversal[A](elements.iterator)

  def apply[A](elements: java.util.Iterator[A]) =
    new Traversal[A](elements.asScala)

  def newBuilder[A]: mutable.Builder[A, Traversal[A]] =
    Iterator.newBuilder[A].mapResult(new Traversal(_))

  def from[A](source: IterableOnce[A]): Traversal[A] =
    new Traversal(Iterator.from(source))

  def fromSingle[A](a: A): Traversal[A] = new Traversal(Iterator.single(a))
}
