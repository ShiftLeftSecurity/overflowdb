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
        val (element, _path) = outerTraversal.elementsWithPath.iterator.next
        //        println(s"nextCur: element=$element; pathTrackingEnabled=$pathTrackingEnabled")
        path = _path.appended(element)
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

    new PathAwareTraversal[B](newIter)
  }


  override def map[B](f: A => B): Traversal[B] =
    new PathAwareTraversal(
        elementsWithPath.iterator.map { case (a, path) =>
        val b = f(a)
        (b, path.appended(b))
      }
    )

  // TODO add type safety once we're on dotty, similar to gremlin-scala's as/label steps with typelevel append
  override def path: Traversal[Seq[Any]] = {
    new Traversal(elementsWithPath.map { case (a, path) => (path :+ a).to(Seq) })
//    val res = this match {
//      case traversal: TraversalPathAware[A] =>
//        traversal.elementsWithPath.map { case (a, path) => (path :+ a).to(Seq) }
//    }
    //    val res = this.asInstanceOf[TraversalPathAware[A]].elementsWithPath.map { case (a, path) =>
    ////      println(s"path: path=${path}")
    //      (path :+ a).to(Seq)
    //    }
//    res
//    ???
  }

}

object PathAwareTraversal {
  def fromSingle[A](a: A): PathAwareTraversal[A] =
    new PathAwareTraversal(Iterator.single((a, Vector.empty)))
}