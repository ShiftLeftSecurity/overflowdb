package overflowdb.traversal

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
  def l: List[A] = iterator.toList

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

  def count: Traversal[Int] =
    Traversal.fromSingle(iterator.size)

  def cast[B]: Traversal[B] =
    mapElements(_.asInstanceOf[B])

  /** Deduplicate elements of this traversal - a.k.a. distinct, unique, ... */
  def dedup: Traversal[A] =
    Traversal(iterator.distinct)

  def dedupBy(fun: A => Any): Traversal[A] =
    Traversal(iterator.distinctBy(fun))

  /** perform side effect without changing the contents of the traversal */
  @Doc("perform side effect without changing the contents of the traversal")
  def sideEffect(fun: A => Unit): Traversal[A] =
    mapElements { a =>
      fun(a)
      a
    }

  /** perform side effect without changing the contents of the traversal
   *  will only apply the partialFunction if it is defined for the given input - analogous to `collect` */
  def sideEffectPF(pf: PartialFunction[A, Unit]): Traversal[A] = map { a =>
    pf.applyOrElse(a, {_: A => ()})
    a
  }

  /** Filter step: only preserves elements if the provided traversal has at least one result.
   * inverse: {{{not/whereNot}}} */
  def where(trav: Traversal[A] => Traversal[_]): Traversal[A] =
    filter { a: A =>
      trav(Traversal.fromSingle(a)).hasNext
    }

  /** Filter step: only preserves elements if the provided traversal does _not_ have any results.
   * inverse: {{{where}}} */
  def whereNot(trav: Traversal[A] => Traversal[_]): Traversal[A] =
    filterNot { a: A =>
      trav(Traversal.fromSingle(a)).hasNext
    }

  /** Filter step: only preserves elements if the provided traversal does _not_ have any results.
   * alias for {{{whereNot}}} */
  def not(trav: Traversal[A] => Traversal[_]): Traversal[A] =
    whereNot(trav)

  /** Filter step: only preserves elements for which _at least one of_ the given traversals has at least one result.
   * Works for arbitrary amount of 'OR' traversals.
   * @example {{{
   *   .or(_.label("someLabel"),
   *       _.has("someProperty"))
   * }}} */
  def or(traversals: (Traversal[A] => Traversal[_])*): Traversal[A] =
    filter { a: A =>
      traversals.exists { trav =>
        trav(Traversal.fromSingle(a)).hasNext
      }
    }

  /** Filter step: only preserves elements for which _all of_ the given traversals has at least one result.
   * Works for arbitrary amount of 'AND' traversals.
   * @example {{{
   *   .and(_.label("someLabel"),
   *        _.has("someProperty"))
   * }}} */
  def and(traversals: (Traversal[A] => Traversal[_])*): Traversal[A] =
    filter { a: A =>
      traversals.forall { trav =>
        trav(Traversal.fromSingle(a)).hasNext
      }
    }

  /** Repeat the given traversal
   *
   * By default it will continue repeating until there's no more results, not emit anything along the way, and use
   * depth first search.
   *
   * The @param behaviourBuilder allows you to configure end conditions (times/until), whether it should emit
   * elements it passes by, and which search algorithm to use (depth-first or breadth-first).
   *
   * Search algorithm: Depth First Search (DFS) vs Breadth First Search (BFS):
   * DFS means the repeat step will go deep before wide. BFS does the opposite: wide before deep.
   * For example, given the graph {{{ L3 <- L2 <- L1 <- Center -> R1 -> R2 -> R3 -> R4 }}}
   * DFS will iterate the nodes in the order: {{{ Center, L1, L2, L3, R1, R2, R3, R4 }}}
   * BFS will iterate the nodes in the order: {{{ Center, L1, R1, R1, R2, L3, R3, R4 }}}
   *
   * @example
   * {{{
   * .repeat(_.out)                            // repeat until there's no more elements, emit nothing, use DFS
   * .repeat(_.out)(_.times(3))                               // perform exactly three repeat iterations
   * .repeat(_.out)(_.until(_.property(Name).endsWith("2")))  // repeat until the 'Name' property ends with '2'
   * .repeat(_.out)(_.emit)                                   // emit everything along the way
   * .repeat(_.out)(_.emit.breadthFirstSearch)                // emit everything, use BFS
   * .repeat(_.out)(_.emit(_.property(Name).startsWith("L"))) // emit if the 'Name' property starts with 'L'
   * }}}
   *
   * @note this works for domain-specific steps as well as generic graph steps - for details please take a look at
   * the examples in RepeatTraversalTests: both '''.followedBy''' and '''.out''' work.
   *
   * @see RepeatTraversalTests for more detail and examples for all of the above.
   */
  def repeat[B >: A](repeatTraversal: Traversal[A] => Traversal[B])
    (implicit behaviourBuilder: RepeatBehaviour.Builder[B] => RepeatBehaviour.Builder[B] = RepeatBehaviour.noop[B] _)
    : Traversal[B] = {
    val behaviour = behaviourBuilder(new RepeatBehaviour.Builder[B]).build
    val _repeatTraversal = repeatTraversal.asInstanceOf[Traversal[B] => Traversal[B]] //this cast usually :tm: safe, because `B` is a supertype of `A`
    flatMap(RepeatStep(_repeatTraversal, behaviour))
  }

  /** Branch step: based on the current element, match on something given a traversal, and provide resulting traversals
   * based on the matched element. Allows to implement conditional semantics: if, if/else, if/elseif, if/elseif/else, ...
   *
   * @param on Traversal to get to what you want to match on
   * @tparam BranchOn required to be >: Null because the implementation is using `null` as the default value. I didn't
   *                  find a better way to implement all semantics with the niceties of PartialFunction, and also yolo...
   * @param options PartialFunction from the matched element to the resulting traversal
   * @tparam NewEnd The element type of the resulting traversal
   *
   * @example
   * {{{
   * .choose(_.property(Name)) {
   *   case "L1" => _.out
   *   case "R1" => _.repeat(_.out)(_.times(3))
   *   case _ => _.in
   * }
   * }}}
   * @see LogicalStepsTests
   */
  def choose[BranchOn >: Null, NewEnd]
    (on: Traversal[A] => Traversal[BranchOn])
    (options: PartialFunction[BranchOn, Traversal[A] => Traversal[NewEnd]]): Traversal[NewEnd] =
    flatMap { a: A =>
      val branchOnValue: BranchOn = on(Traversal.fromSingle(a)).headOption.getOrElse(null)
      if (options.isDefinedAt(branchOnValue)) {
        options(branchOnValue)(Traversal.fromSingle(a))
      } else {
        Traversal.empty
      }
    }

  /** Branch step: evaluates the provided traversals in order and returns the first traversal that emits at least one element.
   *
   * @example
   * {{{
   *  .coalesce(
   *    _.out("label1"),
   *    _.in("label2"),
   *    _.in("label3")
   *  )
   * }}}
   * @see LogicalStepsTests
   */
  def coalesce[NewEnd](options: (Traversal[A] => Traversal[NewEnd])*): Traversal[NewEnd] =
    flatMap { a: A =>
      options.iterator.map(_.apply(Traversal.fromSingle(a))).collectFirst {
        case option if option.nonEmpty => option
      }.getOrElse(Traversal.empty)
    }

  def enablePathTracking: PathAwareTraversal[A] =
    PathAwareTraversal.from(elements)

  def path: Traversal[Vector[Any]] =
    throw new AssertionError("path tracking not enabled, please make sure you have a `PathAwareTraversal`, e.g. via `Traversal.enablePathTracking`")

  /** Removes all results whose traversal path has repeated objects.
   * prerequisite: {{{enablePathTracking}}} */
  def simplePath: Traversal[A] =
    throw new AssertionError("path tracking not enabled, please make sure you have a `PathAwareTraversal`, e.g. via `Traversal.enablePathTracking`")

  /** create a new Traversal instance with mapped elements
   * only exists so it can be overridden by extending classes (e.g. {{{PathAwareTraversal}}}) */
  protected def mapElements[B](f: A => B): Traversal[B] =
    new Traversal(iterator.map(f))

  override val iterator: Iterator[A] = elements.iterator
  override def toIterable: Iterable[A] = Iterable.from(elements)
  override def iterableFactory: IterableFactory[Traversal] = Traversal
  override def toString = getClass.getSimpleName
  override protected def coll: Traversal[A] = this
}

object Traversal extends IterableFactory[Traversal] {
  protected val logger = LoggerFactory.getLogger("Traversal")

  /* reconfigure with different base package if needed */
  var help = new TraversalHelp("overflowdb")

  override def empty[A]: Traversal[A] = new Traversal(Iterator.empty)

  def fromSingle[A](a: A): Traversal[A] =
    new Traversal(Iterator.single(a))

  def apply[A](iterable: IterableOnce[A]) =
    from(iterable)

  override def from[A](iterable: IterableOnce[A]): Traversal[A] =
    iterable match {
      case traversal: Traversal[A] => traversal
      case _ => new Traversal(iterable)
    }

  def apply[A](iterable: java.util.Iterator[A]): Traversal[A] =
    from(iterable.asScala)

  override def newBuilder[A]: mutable.Builder[A, Traversal[A]] =
    Iterator.newBuilder[A].mapResult(new Traversal(_))

}
