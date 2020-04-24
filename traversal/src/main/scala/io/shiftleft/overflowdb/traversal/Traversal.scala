package io.shiftleft.overflowdb.traversal

import io.shiftleft.overflowdb.traversal
import io.shiftleft.overflowdb.traversal.help.{Doc, TraversalHelp}
import org.slf4j.LoggerFactory

import scala.collection.immutable.{ArraySeq, IndexedSeq}
import scala.collection.{Iterable, IterableFactory, IterableFactoryDefaults, IterableOnce, IterableOps, Iterator, mutable}
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

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

  /** Execute the traversal and convert the result to a list - shorthand for `toList` */
  @Doc("Execute the traversal and convert the result to a list - shorthand for `toList`")
  def l: IndexedSeq[A] = elements.iterator.to(ArraySeq.untagged)

  def iterate: Unit = while (hasNext) next

  /**
   * Print help/documentation based on the current elementType `A`.
   * Relies on all step extensions being annotated with @TraversalExt / @Doc
   * Note that this works independently of tab completion and implicit conversions in scope - it will simply list
   * all documented steps in the classpath
   * */
  def help()(implicit elementType: ClassTag[A]): String =
    Traversal.help.renderTable(elementType.runtimeClass, verbose = false)

  def helpVerbose()(implicit elementType: ClassTag[A]): String =
    Traversal.help.renderTable(elementType.runtimeClass, verbose = true)

  def cast[B]: Traversal[B] =
    new Traversal[B](elements.iterator.map(_.asInstanceOf[B]))

  /** perform side effect without changing the contents of the traversal */
  @Doc("perform side effect without changing the contents of the traversal")
  def sideEffect(fun: A => Unit): Traversal[A] = map { a =>
    fun(a)
    a
  }

  /** perform side effect without changing the contents of the traversal
   *  will only apply the partialFunction if it is defined for the given input - analogous to `collect` */
  def sideEffectPF(pf: PartialFunction[A, Unit]): Traversal[A] = map { a =>
    pf.applyOrElse(a, {_: A => ()})
    a
  }

  def repeat(repeatTraversal: Traversal[A] => Traversal[A],
             behaviourBuilder: RepeatBehaviour.Builder[A] => RepeatBehaviour.Builder[A] = identity)
    : Traversal[A] = {
    val behaviour = behaviourBuilder(new traversal.RepeatBehaviour.Builder[A]).build
    _repeat(repeatTraversal, behaviour, currentDepth = 0, emitSack = mutable.ListBuffer.empty)
  }

  private def _repeat(repeatTraversal: Traversal[A] => Traversal[A],
                      behaviour: RepeatBehaviour[A],
                      currentDepth: Int,
                      emitSack: mutable.ListBuffer[A]): Traversal[A] = {
    if (isEmpty || behaviour.timesReached(currentDepth)) {
      // we're at the end - emit whatever we collected on the way plus the current position
      (emitSack.iterator ++ this).to(Traversal)
    } else {
      traversalConsideringEmit(behaviour, emitSack).flatMap { element =>
        if (behaviour.untilCondition.isDefined && behaviour.untilCondition.get.apply(element)) {
          // `until` condition reached - finishing the repeat traversal here, emitting the current element and the emitSack (if any)
          Traversal.from(emitSack, element)
        } else {
          val aLifted = Traversal.fromSingle(element)
          repeatTraversal(aLifted)._repeat(repeatTraversal, behaviour, currentDepth + 1, emitSack)
        }
      }
    }
  }

  private def traversalConsideringEmit(behaviour: RepeatBehaviour[A], emitSack: mutable.ListBuffer[A]): Traversal[A] =
    behaviour match {
      case _: EmitNothing    => this
      case _: EmitEverything => this.sideEffect(emitSack.addOne(_))
      case conditional: EmitConditional[A] =>
        this.sideEffect { a =>
          if (conditional.emit(a)) emitSack.addOne(a)
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

  /* reconfigure with different base package if needed */
  var help = new TraversalHelp("io.shiftleft")

  override def empty[A]: Traversal[A] = new Traversal(Iterator.empty)

  def apply[A](elements: IterableOnce[A]) = new Traversal[A](elements.iterator)

  def apply[A](elements: java.util.Iterator[A]) =
    new Traversal[A](elements.asScala)

  override def newBuilder[A]: mutable.Builder[A, Traversal[A]] =
    Iterator.newBuilder[A].mapResult(new Traversal(_))

  override def from[A](iter: IterableOnce[A]): Traversal[A] =
    new Traversal(Iterator.from(iter))

  def from[A](iter: IterableOnce[A], a: A): Traversal[A] = {
    val builder = Traversal.newBuilder[A]
    builder.addAll(iter)
    builder.addOne(a)
    builder.result
  }

  def fromSingle[A](a: A): Traversal[A] = new Traversal(Iterator.single(a))
}
