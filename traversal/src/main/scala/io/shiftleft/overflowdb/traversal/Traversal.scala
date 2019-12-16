package io.shiftleft.overflowdb.traversal

import io.shiftleft.overflowdb.traversal
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.immutable.{ArraySeq, IndexedSeq}
import scala.collection.{AbstractIterator, Iterable, IterableFactory, IterableFactoryDefaults, IterableOnce, IterableOnceOps, IterableOps, Iterator, mutable}
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

  def hasNext: Boolean = iterator.hasNext
  def next: A = iterator.next
  def nextOption: Option[A] = iterator.nextOption
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
    _repeat(repeatTraversal, behaviour, currentDepth = 0, emitSack = mutable.ListBuffer.empty)
  }

//  @tailrec
  private def _repeat(repeatTraversal: Traversal[A] => Traversal[A],
                      behaviour: RepeatBehaviour[A],
                      currentDepth: Int,
                      emitSack: mutable.ListBuffer[A]): Traversal[A] = {
    val empty = this.isEmpty
    val maxDepthReached = behaviour.timesReached(currentDepth)
//    println(s"_repeat: empty=$empty, maxDepthReached=$maxDepthReached")
    if (empty || maxDepthReached) {
      // we're at the end - emit whatever we collected on the way plus the current position
      println("we're at the end")
      (emitSack.iterator ++ this).to(Traversal)
    } else {
      val traversalConsideringEmit = behaviour match {
        case _: EmitNothing    => this
        case _: EmitEverything => this.sideEffect(emitSack.addOne(_))
        case conditional: EmitConditional[A] => this.sideEffect { a =>
          if (conditional.emit(a)) emitSack.addOne(a)
        }
      }
      val traversalConsideringUntil = behaviour.untilCondition match {
        case None => traversalConsideringEmit
        case Some(condition) => traversalConsideringEmit.filterNot(condition)
      }
//      repeatTraversal(traversalConsideringUntil)._repeat(repeatTraversal, behaviour, currentDepth + 1, emitSack)
//      repeatTraversal(traversalConsideringUntil).iterator.flatMap { a =>
//        Traversal.fromSingle(a)._repeat(repeatTraversal, behaviour, currentDepth + 1, emitSack) //.to(Traversal)
//      }.to(Traversal)
//      repeatTraversal(this)._repeat(repeatTraversal, behaviour, currentDepth + 1, emitSack)
      traversalConsideringUntil.flatMap { a =>
        val aLifted = Traversal.fromSingle(a)
        repeatTraversal(aLifted)._repeat(repeatTraversal, behaviour, currentDepth + 1, emitSack)
      }
    }
  }

  def repeat2(repeatTraversal: Traversal[A] => Traversal[A],
             behaviourBuilder: RepeatBehaviour.Builder[A] => RepeatBehaviour.Builder[A] = identity)
  : Traversal[A] = {
    val behaviour = behaviourBuilder(new traversal.RepeatBehaviour.Builder[A]).build
    _repeat2(repeatTraversal, behaviour, currentDepth = 0, emitSack = mutable.ListBuffer.empty)
  }

  //  @tailrec
  private def _repeat2(repeatTraversal: Traversal[A] => Traversal[A],
                      behaviour: RepeatBehaviour[A],
                      currentDepth: Int,
                      emitSack: mutable.ListBuffer[A]): Traversal[A] = {
    val empty = this.isEmpty
    val timesReached = behaviour.timesReached(currentDepth)
        println(s"_repeat: empty=$empty, timesReached=$timesReached")
    if (empty || timesReached) {
      // we're at the end - emit whatever we collected on the way plus the current position
      println("reached the end of Traversal")
      (emitSack.iterator ++ this).to(Traversal)
    } else {
//      repeatTraversal(this)._repeat2(repeatTraversal, behaviour, currentDepth + 1, emitSack)
      this.flatMap { a =>
        val aLifted = Traversal.fromSingle(a)
        repeatTraversal(aLifted)._repeat2(repeatTraversal, behaviour, currentDepth + 1, emitSack)
      }
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
