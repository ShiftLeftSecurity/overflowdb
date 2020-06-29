package overflowdb.traversal

import java.util.concurrent.atomic.AtomicInteger

import org.slf4j.LoggerFactory
import overflowdb.traversal.help.{Doc, TraversalHelp}

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
  def l: Seq[A] = elements.iterator.toSeq

  def iterate: Unit = while (hasNext) next

  /**
   * Print help/documentation based on the current elementType `A`.
   * Relies on all step extensions being annotated with @TraversalExt / @Doc
   * Note that this works independently of tab completion and implicit conversions in scope - it will simply list
   * all documented steps in the classpath
   * */
  def help()(implicit elementType: ClassTag[A]): String =
    Traversal.help.forElementSpecificSteps(elementType.runtimeClass, verbose = false)

  def helpVerbose()(implicit elementType: ClassTag[A]): String =
    Traversal.help.forElementSpecificSteps(elementType.runtimeClass, verbose = true)

  def count: Int =
    elements.iterator.size

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

  /**
   * filters out objects from the traversal stream when the traversal provided as an argument returns an object.
   * inverse of {{{not}}}
   */
  def where(trav: A => Traversal[_]): Traversal[A] =
    filter { a: A =>
      trav(a).hasNext
    }

  /**
   * filters out objects from the traversal stream when the traversal provided as an argument returns an object.
   * inverse of {{{where}}}
   */
  def not(trav: A => Traversal[_]): Traversal[A] =
    filterNot { a: A =>
      trav(a).hasNext
    }

  final def repeatX[B >: A](repeatTraversal: A => Traversal[B])
                           (implicit behaviourBuilder: RepeatBehaviour.Builder[B] => RepeatBehaviour.Builder[B] = RepeatBehaviour.noop[B] _)
  : Traversal[B] = {
    import RepeatBehaviour.SearchAlgorithm._
    val behaviour = behaviourBuilder(new RepeatBehaviour.Builder[B]).build
    val _repeatTraversal = repeatTraversal.asInstanceOf[B => Traversal[B]] //this cast is usually :tm: safe, because `B` is a supertype of `A`
    behaviour.searchAlgorithm match {
      case DepthFirstSearch => repeatDfs(_repeatTraversal, behaviour)
      case BreadthFirstSearch => repeatBfs(_repeatTraversal, behaviour)
    }
  }

  private def repeatDfs[B >: A](repeatTraversal: B => Traversal[B], behaviour: RepeatBehaviour[B]) : Traversal[B] = {
    lazy val repeatCount = behaviour.times.get
    flatMap { element: B =>
        // TODO refactor/cleanup: extract to RepeatDfsIterator?
        Traversal(new Iterator[B]{
          var buffer: Traversal[B] = Traversal.empty
          var emitSack: List[B] = Nil
          var exhausted = false

          override def hasNext: Boolean = {
            if (buffer.isEmpty) attemptFillBuffer
            buffer.nonEmpty
          }

          override def next: B =
            buffer.head

          private def attemptFillBuffer: Unit = {
            synchronized {
              if (buffer.isEmpty) {
                if (emitSack.nonEmpty) {
                  buffer = Traversal(emitSack)
                  emitSack = Nil
                } else if (!exhausted) {
                  exhausted = true
                  buffer = (0 until repeatCount).foldLeft(Traversal.fromSingle(element)){ (traversal, _) =>
                    traversalConsideringEmit(traversal)
                  }
                }
              }
            }
          }

          private def traversalConsideringEmit(traversal: Traversal[B]): Traversal[B] = {
            import RepeatBehaviour._
            behaviour match {
              case _: EmitNothing    => traversal.flatMap(repeatTraversal)
              case _: EmitAll => traversal.sideEffect(addToSack).flatMap(repeatTraversal)
              // TODO bring in other cases of `traversalConsideringEmit`
//              case _: EmitNothing    => this
//              case _: EmitAll => this.sideEffect(emitSack.addOne(_))
//              case _: EmitAllButFirst if currentDepth > 0 => traversal.sideEffect(emitSack.addOne(_))
//              case _: EmitAllButFirst => this
//              case conditional: EmitConditional[A] =>
//                this.sideEffect { a =>
//                  if (conditional.emit(a)) emitSack.addOne(a)
//                }
            }

          }

          private def addToSack(element: B): Unit =
            emitSack = emitSack :+ element
        })
    }
  }

  private def repeatBfs[B >: A](repeatTraversal: B => Traversal[B], behaviour: RepeatBehaviour[B]) : Traversal[B] = {
    lazy val repeatCount = behaviour.times.get
    flatMap { element: B =>
      Traversal((0 until repeatCount).foldLeft(List(element)){ (trav, _) =>
        trav.flatMap(repeatTraversal)
      })
    }
  }

  def repeat[B >: A](repeatTraversal: Traversal[A] => Traversal[B])
                    (implicit behaviourBuilder: RepeatBehaviour.Builder[B] => RepeatBehaviour.Builder[B] = RepeatBehaviour.noop[B] _)
  : Traversal[B] = {
    val behaviour = behaviourBuilder(new RepeatBehaviour.Builder[B]).build
    _repeat(
      repeatTraversal.asInstanceOf[Traversal[B] => Traversal[B]], //this cast is usually :tm: safe, because `B` is a supertype of `A`
      behaviour,
      currentDepth = 0,
      emitSack = mutable.ListBuffer.empty)
  }

  private def _repeat[B >: A](repeatTraversal: Traversal[B] => Traversal[B],
                      behaviour: RepeatBehaviour[B],
                      currentDepth: Int,
                      emitSack: mutable.ListBuffer[B]): Traversal[B] = {
    if (isEmpty || behaviour.timesReached(currentDepth)) {
      // we're at the end - emit whatever we collected on the way plus the current position
      (emitSack.iterator ++ this).to(Traversal)
    } else {
      traversalConsideringEmit(behaviour, emitSack, currentDepth).flatMap { element =>
        if (behaviour.untilCondition.isDefined && behaviour.untilCondition.get.apply(element)) {
          // `until` condition reached - finishing the repeat traversal here, emitting the current element and the emitSack (if any)
          Traversal.from(emitSack, element)
        } else {
          val bLifted = Traversal.fromSingle(element)
          repeatTraversal(bLifted)._repeat(repeatTraversal, behaviour, currentDepth + 1, emitSack)
        }
      }
    }
  }

  private def traversalConsideringEmit[B >: A](behaviour: RepeatBehaviour[B], emitSack: mutable.ListBuffer[B], currentDepth: Int): Traversal[B] = {
    import RepeatBehaviour._
    behaviour match {
      case _: EmitNothing    => this
      case _: EmitAll => this.sideEffect(emitSack.addOne(_))
      case _: EmitAllButFirst if currentDepth > 0 => this.sideEffect(emitSack.addOne(_))
      case _: EmitAllButFirst => this
      case conditional: EmitConditional[A] =>
        this.sideEffect { a =>
          if (conditional.emit(a)) emitSack.addOne(a)
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
          Traversal.logger.warn("Traversal already exhausted")
        else isExhausted = true
      }
//      println(s"XXX hasNext=${_hasNext}")
      GlobalCounter.hasNextInvocationCount.incrementAndGet()
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
  var help = new TraversalHelp("overflowdb")

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

object GlobalCounter {
  var hasNextInvocationCount = new AtomicInteger(0)
}
