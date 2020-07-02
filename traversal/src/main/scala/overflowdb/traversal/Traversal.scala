package overflowdb.traversal

import java.util.concurrent.atomic.AtomicInteger

import org.slf4j.LoggerFactory
import overflowdb.traversal.RepeatBehaviour.SearchAlgorithm._
import overflowdb.traversal.RepeatBehaviour._
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

  /**
   * Repeat the given traversal
   * By default it will continue repeating until there's no more results, not emit anything along the way, and use a
   * 'depth first search' (DFS) algorithm, i.e. go deep before wide.
   *
   * The @param behaviourBuilder allows you to configure all of the above - here are some typical use cases:
   * {{{
   * .repeat(_.out)(_.times(3))                               // perform exactly three repeat iterations
   * .repeat(_.out)(_.until(_.property(Name).endsWith("2")))  // repeat until the 'Name' property ends with '2'
   * .repeat(_.out)(_.emit)                                   // emit everything along the way
   * .repeat(_.out)(_.emit.breadthFirstSearch)                // emit everything, use BFS
   * .repeat(_.out)(_.emit(_.property(Name).startsWith("L"))) // emit if the 'Name' property starts with 'L'
   * }}}
   * See RepeatTraversalTests for more examples!
   *
   * Note that this works for domain-specific steps as well as generic graph steps - for details please take a look at
   * the examples in RepeatTraversalTests: both {{{.followedBy}}} and {{{.out}}} work.
   */
  final def repeat[B >: A](repeatTraversal: A => Traversal[B])
    (implicit behaviourBuilder: RepeatBehaviour.Builder[B] => RepeatBehaviour.Builder[B] = RepeatBehaviour.noop[B] _)
    : Traversal[B] = {
    val behaviour = behaviourBuilder(new RepeatBehaviour.Builder[B]).build
    val _repeatTraversal = repeatTraversal.asInstanceOf[B => Traversal[B]] //this cast usually :tm: safe, because `B` is a supertype of `A`
    behaviour.searchAlgorithm match {
      case DepthFirstSearch => repeatDfs(_repeatTraversal, behaviour)
      case BreadthFirstSearch => repeatBfs(_repeatTraversal, behaviour)
    }
  }

  private def repeatDfs[B >: A](repeatTraversal: B => Traversal[B], behaviour: RepeatBehaviour[B]) : Traversal[B] =
    flatMap(RepeatStep.DepthFirst(repeatTraversal, behaviour))

  private def repeatBfs[B >: A](repeatTraversal: B => Traversal[B], behaviour: RepeatBehaviour[B]) : Traversal[B] =
    flatMap(RepeatStep.BreadthFirst(repeatTraversal, behaviour))

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
