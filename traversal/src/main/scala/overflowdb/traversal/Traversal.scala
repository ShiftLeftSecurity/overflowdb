package overflowdb.traversal

import org.slf4j.LoggerFactory
import overflowdb.traversal.help.{Doc, DocSearchPackages, TraversalHelp}

import scala.collection.{
  Iterable,
  IterableFactory,
  IterableFactoryDefaults,
  IterableOnce,
  IterableOps,
  Iterator,
  mutable
}
import scala.collection.immutable.ArraySeq
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

class TraversalSugarExt[A](val iterator: Iterator[A]) extends AnyVal {
  type Traversal[A] = Iterator[A]

  /** Execute the traversal and convert the result to a list - shorthand for `toList` */
  @Doc(info = "Execute the traversal and convert the result to a list - shorthand for `toList`")
  def l: List[A] = iterator.toList

  /** Execute the traversal and return a mutable.Set (better performance than `immutableSet`) */
  def toSetMutable[B >: A]: mutable.Set[B] = mutable.Set.from(iterator)

  /** Execute the traversal and convert the result to an immutable Set */
  def toSetImmutable[B >: A]: Set[B] = iterator.toSet

  /** Execute the traversal without returning anything */
  @Doc(info = "Execute the traversal without returning anything")
  def iterate(): Unit =
    while (iterator.hasNext) iterator.next()

  def countTrav: Traversal[Int] =
    Iterator.single(iterator.size)

  def head: A = iterator.next()

  def headOption: Option[A] = iterator.nextOption()

  def last: A = {
    iterator.hasNext
    var res = iterator.next()
    while (iterator.hasNext) res = iterator.next()
    res
  }

  def lastOption: Option[A] =
    if (iterator.hasNext) Some(last) else None

  /** casts all elements to given type note: this can lead to casting errors
    *
    * @see
    *   {{{collectAll}}} as a safe alternative
    */
  @Doc(info = "casts all elements to given type")
  def cast[B]: Traversal[B] =
    iterator.asInstanceOf[Traversal[B]]

  /** collects all elements of the given class (beware of type-erasure) */
  @Doc(info = "collects all elements of the provided class (beware of type-erasure)")
  def collectAll[B](implicit ev: ClassTag[B]): Traversal[B] =
    iterator.filter(ev.runtimeClass.isInstance).asInstanceOf[Traversal[B]]

  /** Deduplicate elements of this traversal - a.k.a. distinct, unique, ... */
  @Doc(info = "deduplicate elements of this traversal - a.k.a. distinct, unique, ...")
  def dedup: Traversal[A] =
    iterator.distinct

  /** deduplicate elements of this traversal by a given function */
  @Doc(info = "deduplicate elements of this traversal by a given function")
  def dedupBy(fun: A => Any): Traversal[A] =
    iterator.distinctBy(fun)

  /** sort elements by their natural order */
  @Doc(info = "sort elements by their natural order")
  def sorted[B >: A](implicit ord: Ordering[B]): Seq[B] = {
    (iterator.to(ArraySeq.untagged): ArraySeq[B]).sorted
  }

  /** sort elements by the value of the given transformation function */
  @Doc(info = "sort elements by the value of the given transformation function")
  def sortBy[B](f: A => B)(implicit ord: Ordering[B]): Seq[A] =
    iterator.to(ArraySeq.untagged).sortBy(f)

  /** Print help/documentation based on the current elementType `A`. Relies on all step extensions being annotated with
    * \@Traversal / @Doc Note that this works independently of tab completion and implicit conversions in scope - it
    * will simply list all documented steps in the classpath
    */
  @Doc(info = "print help/documentation based on the current elementType `A`.")
  def help[B >: A](implicit elementType: ClassTag[B], searchPackages: DocSearchPackages): String =
    new TraversalHelp(searchPackages).forElementSpecificSteps(elementType.runtimeClass, verbose = false)

  @Doc(info = "print verbose help/documentation based on the current elementType `A`.")
  def helpVerbose[B >: A](implicit elementType: ClassTag[B], searchPackages: DocSearchPackages): String =
    new TraversalHelp(searchPackages).forElementSpecificSteps(elementType.runtimeClass, verbose = true)
}
class TraversalFilterExt[A](val iterator: Iterator[A]) extends AnyVal {
  type Traversal[A] = Iterator[A]

  /** filters out everything that is _not_ the given value */
  @Doc(info = "filters out everything that is _not_ the given value")
  def is[B >: A](value: B): Traversal[A] =
    iterator.filter(_ == value)

  /** filters out all elements that are _not_ in the provided set */
  @Doc(info = "filters out all elements that are _not_ in the provided set")
  def within[B >: A](values: Set[B]): Traversal[A] =
    iterator.filter(values.contains)

  /** filters out all elements that _are_ in the provided set */
  @Doc(info = "filters out all elements that _are_ in the provided set")
  def without[B >: A](values: Set[B]): Traversal[A] =
    iterator.filterNot(values.contains)

}

class TraversalLogicExt[A](val iterator: Iterator[A]) extends AnyVal {
  type Traversal[A] = Iterator[A]

  /** perform side effect without changing the contents of the traversal */
  @Doc(info = "perform side effect without changing the contents of the traversal")
  def sideEffect(fun: A => _): Traversal[A] =
    iterator match {
      case pathAwareTraversal: PathAwareTraversal[A] => pathAwareTraversal._sideEffect(fun)
      case _ =>
        iterator.map { a => fun(a); a }
    }

  /** perform side effect without changing the contents of the traversal will only apply the partialFunction if it is
    * defined for the given input - analogous to `collect`
    */
  @Doc(info = "perform side effect without changing the contents of the traversal")
  def sideEffectPF(pf: PartialFunction[A, _]): Traversal[A] =
    sideEffect(pf.lift)

  /** only preserves elements if the provided traversal has at least one result */
  @Doc(info = "only preserves elements if the provided traversal has at least one result")
  def where(trav: Traversal[A] => Traversal[_]): Traversal[A] =
    iterator.filter { (a: A) =>
      trav(Iterator.single(a)).hasNext
    }

  /** only preserves elements if the provided traversal does _not_ have any results */
  @Doc(info = "only preserves elements if the provided traversal does _not_ have any results")
  def whereNot(trav: Traversal[A] => Traversal[_]): Traversal[A] =
    iterator.filter { (a: A) =>
      !trav(Iterator.single(a)).hasNext
    }

  /** only preserves elements if the provided traversal does _not_ have any results - alias for whereNot */
  @Doc(info = "only preserves elements if the provided traversal does _not_ have any results - alias for whereNot")
  def not(trav: Traversal[A] => Traversal[_]): Traversal[A] =
    whereNot(trav)

  /** only preserves elements for which _at least one of_ the given traversals has at least one result Works for
    * arbitrary amount of 'OR' traversals.
    *
    * @example
    *   {{{.or(_.label("someLabel"), _.has("someProperty"))}}}
    */
  @Doc(info = "only preserves elements for which _at least one of_ the given traversals has at least one result")
  def or(traversals: (Traversal[A] => Traversal[_])*): Traversal[A] = {
    iterator.filter { (a: A) =>
      traversals.exists { trav =>
        trav(Iterator.single(a)).hasNext
      }
    }
  }

  /** only preserves elements for which _all of_ the given traversals have at least one result Works for arbitrary
    * amount of 'AND' traversals.
    *
    * @example
    *   {{{.and(_.label("someLabel"), _.has("someProperty"))}}}
    */
  @Doc(info = "only preserves elements for which _all of_ the given traversals have at least one result")
  def and(traversals: (Traversal[A] => Traversal[_])*): Traversal[A] = {
    iterator.filter { (a: A) =>
      traversals.forall { trav =>
        trav(Iterator.single(a)).hasNext
      }
    }
  }

  /** union step from the current point
    *
    * @param traversals
    *   to be executed from here, results are being aggregated/summed/unioned
    * @example
    *   {{{.union(_.out, _.in)}}}
    */
  @Doc(info = "union/sum/aggregate/join given traversals from the current point")
  def union[B](traversals: (Traversal[A] => Traversal[B])*): Traversal[B] = iterator match {
    case pathAwareTraversal: PathAwareTraversal[A] => pathAwareTraversal._union(traversals: _*)
    case _ =>
      iterator.flatMap { (a: A) =>
        traversals.flatMap(_.apply(Iterator.single(a)))
      }
  }

  /** Branch step: based on the current element, match on something given a traversal, and provide resulting traversals
    * based on the matched element. Allows to implement conditional semantics: if, if/else, if/elseif, if/elseif/else,
    * ...
    *
    * @param on
    *   Traversal to get to what you want to match on
    * @tparam BranchOn
    *   required to be >: Null because the implementation is using `null` as the default value. I didn't find a better
    *   way to implement all semantics with the niceties of PartialFunction, and also yolo...
    * @param options
    *   PartialFunction from the matched element to the resulting traversal
    * @tparam NewEnd
    *   The element type of the resulting traversal
    * @example
    *   {{{
    * .choose(_.property(Name)) {
    *   case "L1" => _.out
    *   case "R1" => _.repeat(_.out)(_.maxDepth(3))
    *   case _ => _.in
    * }
    *   }}}
    * @see
    *   LogicalStepsTests
    */
  @Doc(info = "allows to implement conditional semantics: if, if/else, if/elseif, if/elseif/else, ...")
  def choose[BranchOn >: Null, NewEnd](
      on: Traversal[A] => Traversal[BranchOn]
  )(options: PartialFunction[BranchOn, Traversal[A] => Traversal[NewEnd]]): Traversal[NewEnd] = iterator match {
    case pathAwareTraversal: PathAwareTraversal[A] => pathAwareTraversal._choose[BranchOn, NewEnd](on)(options)
    case _ =>
      iterator.flatMap { (a: A) =>
        val branchOnValue: BranchOn = on(Iterator.single(a)).nextOption().getOrElse(null)
        options
          .applyOrElse(branchOnValue, (failState: BranchOn) => ((unused: Traversal[A]) => Iterator.empty[NewEnd]))
          .apply(Iterator.single(a))
      }
  }

  @Doc(info =
    "evaluates the provided traversals in order and returns the first traversal that emits at least one element"
  )
  def coalesce[NewEnd](options: (Traversal[A] => Traversal[NewEnd])*): Traversal[NewEnd] = iterator match {
    case pathAwareTraversal: PathAwareTraversal[A] => pathAwareTraversal._coalesce(options: _*)
    case _ =>
      iterator.flatMap { (a: A) =>
        options.iterator
          .map(_.apply(Iterator.single(a)))
          .collectFirst {
            case option if option.nonEmpty => option
          }
          .getOrElse(Iterator.empty)
      }
  }

  /*
  /** aggregate all objects at this point into the given collection (side effect)
   *
   * @example
   *   {{{
   *  val xs = mutable.ArrayBuffer.empty[A]
   *  myTraversal.aggregate(xs).foo.bar
   *  // xs will be filled once `myTraversal` is executed
   *   }}}
   */
  @Doc(info = "aggregate all objects at this point into the given collection (side effect)")
  def aggregate(into: mutable.Growable[A]): Traversal[A] =
    sideEffect(into.addOne(_))

  /** group elements and count how often they appear */
  @Doc(info = "group elements and count how often they appear")
  def groupCount[B >: A]: Map[B, Int] =
    groupCount(identity[A])

  /** group elements by a given transformation function and count how often the results appear */
  @Doc(info = "group elements by a given transformation function and count how often the results appear")
  def groupCount[B](by: A => B): Map[B, Int] = {
    val counts = mutable.Map.empty[B, Int].withDefaultValue(0)
    iterator.foreach { a =>
      val b = by(a)
      val newValue = counts(b) + 1
      counts.update(b, newValue)
    }
    counts.to(Map)
  }

   */
}

class TraversalTrackingExt[A](val iterator: Iterator[A]) extends AnyVal {
  type Traversal[A] = Iterator[A]

  @Doc(info = "enable path tracking - prerequisite for path/simplePath steps")
  def enablePathTracking: PathAwareTraversal[A] =
    iterator match {
      case pathAwareTraversal: PathAwareTraversal[_] => throw new RuntimeException("path tracking is already enabled")
      case _ => new PathAwareTraversal[A](iterator.map { a => (a, Vector.empty) })
    }

  @Doc(info = "enable path tracking - prerequisite for path/simplePath steps")
  def discardPathTracking: Traversal[A] =
    iterator match {
      case pathAwareTraversal: PathAwareTraversal[A] => pathAwareTraversal.wrapped.map { _._1 }
      case _                                         => iterator
    }

  def isPathTracking: Boolean = iterator.isInstanceOf[PathAwareTraversal[_]]

  /** retrieve entire path that has been traversed thus far prerequisite: enablePathTracking has been called previously
    *
    * @example
    *   {{{
    *  myTraversal.enablePathTracking.out.out.path.toList
    *   }}}
    *   TODO would be nice to preserve the types of the elements, at least if they have a common supertype
    */
  @Doc(info = "retrieve entire path that has been traversed thus far")
  def path: Traversal[Vector[Any]] = iterator match {
    case tracked: PathAwareTraversal[A] =>
      tracked.wrapped.map { case (a, p) =>
        p.appended(a)
      }
    case _ =>
      throw new AssertionError(
        "path tracking not enabled, please make sure you have a `PathAwareTraversal`, e.g. via `Traversal.enablePathTracking`"
      )
  } // fixme: I think ClassCastException is the correct result when the user forgot to enable path tracking. But a better error message to go along with it would be nice.

  def simplePath: Traversal[A] = iterator match {
    case tracked: PathAwareTraversal[A] =>
      new PathAwareTraversal(tracked.wrapped.filter { case (a, p) =>
        mutable.Set.from(p).addOne(a).size == 1 + p.size
      })
    case _ =>
      throw new AssertionError(
        "path tracking not enabled, please make sure you have a `PathAwareTraversal`, e.g. via `Traversal.enablePathTracking`"
      )
  }
}

class TraversalRepeatExt[A](val trav: Iterator[A]) extends AnyVal {
  type Traversal[A] = Iterator[A]

  /** Repeat the given traversal
    *
    * By default it will continue repeating until there's no more results, not emit anything along the way, and use
    * depth first search.
    *
    * The @param behaviourBuilder allows you to configure end conditions (until|whilst|maxDepth), whether it should emit
    * elements it passes by, and which search algorithm to use (depth-first or breadth-first).
    *
    * Search algorithm: Depth First Search (DFS) vs Breadth First Search (BFS): DFS means the repeat step will go deep
    * before wide. BFS does the opposite: wide before deep. For example, given the graph
    * {{{L3 <- L2 <- L1 <- Center -> R1 -> R2 -> R3 -> R4}}} DFS will iterate the nodes in the order:
    * {{{Center, L1, L2, L3, R1, R2, R3, R4}}} BFS will iterate the nodes in the order:
    * {{{Center, L1, R1, R1, R2, L3, R3, R4}}}
    *
    * @example
    *   {{{
    * .repeat(_.out)                            // repeat until there's no more elements, emit nothing, use DFS
    * .repeat(_.out)(_.maxDepth(3))                            // perform exactly three repeat iterations
    * .repeat(_.out)(_.until(_.property(Name).endsWith("2")))  // repeat until the 'Name' property ends with '2'
    * .repeat(_.out)(_.emit)                                   // emit everything along the way
    * .repeat(_.out)(_.emit.breadthFirstSearch)                // emit everything, use BFS
    * .repeat(_.out)(_.emit(_.property(Name).startsWith("L"))) // emit if the 'Name' property starts with 'L'
    *   }}}
    * @note
    *   this works for domain-specific steps as well as generic graph steps - for details please take a look at the
    *   examples in RepeatTraversalTests: both '''.followedBy''' and '''.out''' work.
    * @see
    *   RepeatTraversalTests for more detail and examples for all of the above.
    */
  // @Doc(info = "repeat the given traversal")
  def repeat[B >: A](
      repeatTraversal: Traversal[A] => Traversal[B]
  )(implicit
      behaviourBuilder: RepeatBehaviour.Builder[B] => RepeatBehaviour.Builder[B] = RepeatBehaviour.noop[B] _
  ): Traversal[B] = {
    val behaviour = behaviourBuilder(new RepeatBehaviour.Builder[B]).build
    val _repeatTraversal =
      repeatTraversal
        .asInstanceOf[Traversal[B] => Traversal[B]] // this cast usually :tm: safe, because `B` is a supertype of `A`
    trav match {
      case tracked: PathAwareTraversal[A] =>
        val step = PathAwareRepeatStep(_repeatTraversal, behaviour)
        new PathAwareTraversal(tracked.wrapped.flatMap { case (a, p) =>
          step.apply(a).wrapped.map { case (aa, pp) => (aa, p ++ pp) }
        })
      case _ => trav.flatMap(RepeatStep(_repeatTraversal, behaviour))

    }
  }
}
