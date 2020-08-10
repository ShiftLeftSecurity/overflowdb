package overflowdb.traversal

import org.slf4j.LoggerFactory
import overflowdb.traversal
import overflowdb.traversal.help.{Doc, TraversalHelp}

import scala.collection.{Iterable, IterableFactory, IterableFactoryDefaults, IterableOnce, IterableOps, Iterator, mutable}
import scala.reflect.ClassTag

class PathAwareTraversal[A](val elementsWithPath: IterableOnce[(A, Vector[Any])]) extends Traversal[A](elementsWithPath.iterator.map(_._1)) {
//  println("PathAwareTraversal:init")

// idea: implement flatmap ourselves, to avoid conversion to/from iterator
// initially copied from Iterator.flatMap
  override def flatMap[B](f: A => IterableOnce[B]): Traversal[B] = {
    val outerTraversal = this
    val newIter = new Iterator[(B, Vector[Any])] {
      private[this] var cur: Traversal[B] = Traversal.empty
      private[this] var _hasNext: Int = -1
      /** Trillium logic boolean: -1 = unknown, 0 = false, 1 = true */
      private[this] var path: Vector[Any] = Vector.empty

      private[this] def nextCur(): Unit = {
        cur = null
        val (a, _path) = outerTraversal.elementsWithPath.iterator.next
        path = _path.appended(a)
        cur = f(a)
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
        (cur.next, path)
      }
    }

    new PathAwareTraversal[B](newIter)
  }

  override def map[B](f: A => B): Traversal[B] =
    new PathAwareTraversal(
      elementsWithPath.iterator.map { case (a, path) =>
        val b = f(a)
        (b, path.appended(b))
      }
    )

  override def collect[B](pf: PartialFunction[A, B]): Traversal[B] =
    new PathAwareTraversal(
      elementsWithPath.iterator.collect { case (a, path) if pf.isDefinedAt(a) =>
        val b = pf(a)
        (b, path.appended(a))}
    )

  override def filter(pred: A => Boolean): Traversal[A] =
    new PathAwareTraversal(
      elementsWithPath.iterator.filter { case (a, path) => pred(a)}
    )

  override def filterNot(pred: A => Boolean): Traversal[A] =
    new PathAwareTraversal(
      elementsWithPath.iterator.filterNot { case (a, path) => pred(a)}
    )

  override def dedup(implicit behaviourBuilder: DedupBehaviour.Builder => DedupBehaviour.Builder): Traversal[A] =
    new PathAwareTraversal(
      behaviourBuilder(new DedupBehaviour.Builder).build.comparisonStyle match {
        case DedupBehaviour.ComparisonStyle.HashAndEquals =>
          elementsWithPath.to(LazyList).distinctBy(_._1)
        case DedupBehaviour.ComparisonStyle.HashOnly =>
          elementsWithPath.to(LazyList).distinctBy(_._1.hashCode)
      }
    )

  // TODO add type safety once we're on dotty, similar to gremlin-scala's as/label steps with typelevel append?
  override def path: Traversal[Seq[Any]] =
    new Traversal(elementsWithPath.map { case (a, path) => (path :+ a).to(Seq) })

}

object PathAwareTraversal {
  def fromSingle[A](a: A): PathAwareTraversal[A] =
    new PathAwareTraversal(Iterator.single((a, Vector.empty)))
}