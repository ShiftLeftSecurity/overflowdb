package overflowdb.traversal

import org.slf4j.LoggerFactory
import overflowdb.traversal.help.{Doc, TraversalHelp}

import scala.collection.{AbstractIterator, Iterable, IterableFactory, IterableFactoryDefaults, IterableOnce, IterableOps, Iterator, View, mutable}
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

object GlobalFoo {
  var afterFirstOutStep = false
}

class TraversalPathAware[A](val elementsWithPath: IterableOnce[(A, Vector[Any])]) extends Traversal[A](elementsWithPath.iterator.map(_._1)) {
//  println("TraversalPathAware:init")
  override def pathTrackingEnabled = true
}

trait PathTrackingSetting {
  def pathTrackingEnabled: Boolean
}

/**
  * TODO more docs
  *
  * Just like Tinkerpop3 and most other Iterators, a Traversal can only be executed once.
  * Since this may trip up users, we'll log a warning
 **/
class Traversal[A](elements: IterableOnce[A])
    extends IterableOnce[A]
    with IterableOps[A, Traversal, Traversal[A]]
    with IterableFactoryDefaults[A, Traversal]
    with PathTrackingSetting {
//  println("Traversal:init")
  val a = 1
  var _pathTrackingEnabled = false
  override def pathTrackingEnabled = _pathTrackingEnabled

// idea: implement flatmap ourselves, to avoid conversion to/from iterator
// initially copied from Iterator.flatMap
  def flatMap3[B](f: A => Traversal[B]): IterableOnce[B] = {
    val outerTraversal = this
    val newIter = new Iterator[(B, Vector[Any])] {
      private[this] var cur: Traversal[B] = Traversal.empty
      private[this] var _hasNext: Int = -1
      /** Trillium logic boolean: -1 = unknown, 0 = false, 1 = true */
      private[this] var path: Vector[Any] = Vector.empty

      private[this] def nextCur(): Unit = {
        cur = null
        val (element, _path) = outerTraversal match {
          case trav: TraversalPathAware[A] => trav.elementsWithPath.iterator.next
          case trav: Traversal[A] =>
//            val element = trav.next
//            (element, Vector(element))
            (trav.next, Vector.empty)
        }
//        println(s"nextCur: element=$element; pathTrackingEnabled=$pathTrackingEnabled")
        if (pathTrackingEnabled)
          path = _path.appended(element)
        else
          path = Vector(element) //otherwise first step will not be tracked
        cur = f(element) //f==out3
        _hasNext = -1
      }

      def hasNext: Boolean = {
        if (_hasNext == -1) {
          while (!cur.hasNext) {
            if (!outerTraversal.hasNext) {
              _hasNext = 0
              cur = Traversal.empty
              return false
            }
            nextCur()
          }
          _hasNext = 1
          true
        } else _hasNext == 1
      }

      def next(): (B, Vector[Any]) = {
        if (hasNext) {
          _hasNext = -1
        }
        val b = cur.next
        (b, path)
      }
    }

  // conversion happens probably when instantiating TraversalPathAware - newIter is converted to
    new TraversalPathAware[B](newIter)
  }

  // TODO add type safety once we're on dotty, similar to gremlin-scala's as/label steps with typelevel append
  def path: Traversal[Seq[Any]] = {
    _pathTrackingEnabled = true
    val res = this.asInstanceOf[TraversalPathAware[A]].elementsWithPath.map { case (a, path) =>
//      println(s"path: path=${path}")
      (path :+ a).to(Seq)
    }
    res._pathTrackingEnabled = true
    res
//        map { a =>
//          println(s"path: _path=${_path}")
//          (_path :+ a).to(Seq)
//          Seq(a)
//        }
  }


  //  // idea: implement flatmap ourselves, to avoid conversion to/from iterator
//  // initially copied from Iterator.flatMap
// def flatMap3[B](f: A => Traversal[B]): IterableOnce[B] = {
//    val oldTraversal = this
//    val newIter = new Iterator[B] {
//      private[this] var cur: Traversal[B] = Traversal.empty
//      /** Trillium logic boolean: -1 = unknown, 0 = false, 1 = true */
//      private[this] var _hasNext: Int = -1
//
//      private[this] def nextCur(): Unit = {
////        val prevPath = cur._path
//        cur = null
//        val a = oldTraversal.next
//        println(s"x0: nextCur: branching at a=$a")
//        cur = f(a) //f==out3
////        GlobalFoo.afterFirstOutStep = true
////        cur._path = prevPath.clone()
//        cur._path = oldTraversal._path.clone
//        cur._path.addOne(s"nextCur: ${a.asInstanceOf[overflowdb.Node].property2("name")}")
//        _hasNext = -1
//      }
//
//      def hasNext: Boolean = {
//        if (_hasNext == -1) {
//          while (!cur.hasNext) {
//            if (!oldTraversal.hasNext) {
//              _hasNext = 0
//              cur = Traversal.empty
//              return false
//            }
//            nextCur()
//          }
//          _hasNext = 1
//          true
//        } else _hasNext == 1
//      }
//
//      def next(): B = {
//        if (hasNext) {
//          _hasNext = -1
//        }
//        val b = cur.next
//        cur._path.addOne(s"next1: ${b.asInstanceOf[overflowdb.Node].property2("name")}")
//        oldTraversal._path.addOne(s"next2: ${b.asInstanceOf[overflowdb.Node].property2("name")}")
//        b
//      }
//    }
//
//    val res = new Traversal[B](newIter)
//    res._path = oldTraversal._path//.clone? // this is where things get flattened and wrong again...
//    res
//  }

//  def flatMap3[B](f: A => Traversal[B]): Traversal[B] = {
////    val oldPath = _path // this one will be updated with the values traversed over within f2
//    //.clone?
//    def f2(a: A, pathPointer: mutable.Buffer[Any]): Traversal[B] = {
////      println(s"flatMap3: _path=${_path}, oldPath=$oldPath")
//      val res = f(a)
//      res._path = _path :+ a
//      pathPointer.addOne(a)
////      println(s"flatMap3: res._path=${res._path}")
//      res
//    }
////    val res = iterator.flatMap(f2)
////    res // this is an iterator and thereby the _path info gets lost,
//    // only to later get converted back into a Traversal without _path
//    // instead: pass in the _newPath pointer
//    // problem: same buffer for all elements - should use new one when branching - similar to first iteration
////    val newPath: mutable.Buffer[Any] = _path.clone
////    val res = new Traversal(iterator.flatMap(a => f2(a, newPath)))
////    res._path = newPath
//
//    val oldPath = _path.toSeq
//    val mappedAndPath = iterator.map { a => (f(a), oldPath :+ a)}
//    val flattenedIter = mappedAndPath.flatten { case (trav, path) => trav._path = path.toBuffer; trav}
//    // unfortunately, the above _is_ an Iterator, and the path gets lost again
//    // idea: copy Iterator.flatMap here and don't use that default impl
//    ???
//
////    new Traversal(iterator.flatMap(f2)) {
////      override protected val _path = oldPath
////    }
//
//    /* idea:
//    val path
//    f3: a: A => Traversal.withPath(oldPath + a)
//    iterator.map { a: A =>
//      f(a).withPath(oldPath + a)
//    }.flatten
//     */
//  }

  //  def flatMap3a[B](f: A => Traversal[B]): Traversal[B] = {
  //    val oldPath = _path//.clone
  //    def f2(a: A): Traversal[B] = {
  //      oldPath.addOne(a)
  //      println(s"flatMap3: _path=${_path}")
  //      f(a)
  //    }
  //    new Traversal(iterator.flatMap(f2)) {
  //      override protected val _path = oldPath
  //    }
  //  }

  def hasNext: Boolean = iterator.hasNext
  def next: A = iterator.next
  def nextOption: Option[A] = iterator.nextOption

  /** Execute the traversal and convert the result to a list - shorthand for `toList` */
  @Doc("Execute the traversal and convert the result to a list - shorthand for `toList`")
  def l: List[A] = elements.iterator.toList

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
    Traversal.fromSingle(elements.iterator.size)

  def cast[B]: Traversal[B] =
    new Traversal[B](elements.iterator.map(_.asInstanceOf[B]))

  /** Deduplicate elements of this traversal - a.k.a. distinct, unique, ...
   * Preserves order and laziness semantics of Traversal.
   *
   * By default, it's determining duplicates based on equals and hashCode, just like java.util.Set.
   * While that's usually fine, be aware that it has to maintain references to those elements even after they've been
   * traversed, i.e. they can't be garbage collected while the traversal has not yet completed. In other words, the
   * semantics are like LazyList, and not like Iterator.
   *
   * It can be configured to determine duplicates based on hashCode only instead, in which case elements can get freed.
   *
   * @example
   * {{{
   * .dedup
   * .dedup(_.hashComparisonOnly)
   * }}}
   *
   * see TraversalTests.scala
   */
  def dedup(implicit behaviourBuilder: DedupBehaviour.Builder => DedupBehaviour.Builder = DedupBehaviour.noop _)
    : Traversal[A] = {
     behaviourBuilder(new DedupBehaviour.Builder).build.comparisonStyle match {
       case DedupBehaviour.ComparisonStyle.HashAndEquals =>
         Traversal(elements.to(LazyList).distinct)
       case DedupBehaviour.ComparisonStyle.HashOnly =>
         Traversal(new DedupByHashIterator(elements))
     }
  }

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

  /** Filter step: only preserves elements if the provided traversal has at least one result.
   * inverse: {{{not}}} */
  def where(trav: Traversal[A] => Traversal[_]): Traversal[A] =
    filter { a: A =>
      trav(Traversal.fromSingle(a)).hasNext
    }

  /** Filter step: only preserves elements if the provided traversal does _not_ have any results.
   * inverse: {{{where}}} */
  def not(trav: Traversal[A] => Traversal[_]): Traversal[A] =
    filterNot { a: A =>
      trav(Traversal.fromSingle(a)).hasNext
    }

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
  final def repeat[B >: A](repeatTraversal: Traversal[A] => Traversal[B])
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

  def apply[A](elements: IterableOnce[A]) = {
    println("Traversal.apply(IterableOnce)")
    new Traversal[A](elements)
  }

  def apply[A](elements: java.util.Iterator[A]) = {
    println("Traversal.apply(j.u.Iterator)")
    new Traversal[A](elements)
  }

  override def newBuilder[A]: mutable.Builder[A, Traversal[A]] = {
    println("Traversal.newBuilder")
    Iterator.newBuilder[A].mapResult(new Traversal(_))
  }

  override def from[A](iter: IterableOnce[A]): Traversal[A] = {
//    println("Traversal.from(IterableOnce)")
    iter match {
      case traversal: Traversal[A] => traversal
      case traversal: PathTrackingSetting =>
        val res = new Traversal(Iterator.from(iter))
        res._pathTrackingEnabled = traversal._pathTrackingEnabled
        res
      case _ => new Traversal(iter)
    }
  }

  def from[A](iter: IterableOnce[A], a: A): Traversal[A] = {
    println("Traversal.from(IterableOnce, A)")
    val builder = Traversal.newBuilder[A]
    builder.addAll(iter)
    builder.addOne(a)
    builder.result
  }

  def fromSingle[A](a: A): Traversal[A] =
    new Traversal(Iterator.single(a))
}
