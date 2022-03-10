package overflowdb.traversal

import scala.annotation.tailrec
import scala.collection.{IterableOnce, Iterator}

class PathAwareTraversal[+A](val elementsWithPath: IterableOnce[(A, Vector[Any])])
  extends Traversal[A](elementsWithPath.iterator.map(_._1)) {

  private val elementsWithPathIterator: Iterator[(A, Vector[Any])] = elementsWithPath.iterator

  override def map[B](f: A => B): Traversal[B] =
    new PathAwareTraversal(
      elementsWithPathIterator.map { case (a, path) =>
        val b = f(a)
        (b, path.appended(a))
      }
    )

  override def flatMap[B](f: A => IterableOnce[B]): Traversal[B] =
    new PathAwareTraversal(
      elementsWithPathIterator.flatMap { case (a, path) =>
        f(a).iterator.map(b => (b, path.appended(a)))
      }
    )

  override def filter(pred: A => Boolean): Traversal[A] =
    new PathAwareTraversal(
      elementsWithPathIterator.filter(x => pred(x._1))
    )

  override def filterNot(pred: A => Boolean): Traversal[A] =
    new PathAwareTraversal(
      elementsWithPathIterator.filterNot(x => pred(x._1))
    )

  override def collect[B](pf: PartialFunction[A, B]): Traversal[B] =
    new PathAwareTraversal(
      elementsWithPathIterator.collect { case (a, path) if pf.isDefinedAt(a) =>
        val b = pf(a)
        (b, path.appended(a))}
    )

  override def dedup: Traversal[A] =
    new PathAwareTraversal(elementsWithPathIterator.distinctBy(_._1))

  override def dedupBy(fun: A => Any): Traversal[A] =
    new PathAwareTraversal(elementsWithPathIterator.distinctBy(x => fun(x._1)))

  // TODO add type safety once we're on dotty, similar to gremlin-scala's as/label steps with typelevel append?
  override def path: Traversal[Vector[Any]] =
    new Traversal(elementsWithPathIterator.map {
      case (a, path) => path.appended(a)
    })

  /** Removes all results whose traversal path has repeated objects. */
  override def simplePath: Traversal[A] =
    new PathAwareTraversal(
      elementsWithPathIterator.filterNot{ case (element, path) =>
        containsDuplicates(path.appended(element))
      }
    )

  @tailrec
  private final def containsDuplicates(seq: Seq[_]): Boolean = {
    if (seq.size <= 1) false
    else {
      val lookingFor = seq.head
      val lookingIn  = seq.tail.iterator
      var foundDuplicate = false
      while (lookingIn.hasNext && !foundDuplicate) {
        if (lookingIn.next() == lookingFor) {
          foundDuplicate = true
        }
      }
      foundDuplicate || containsDuplicates(seq.tail)
    }
  }

  override def repeat[B >: A](repeatTraversal: Traversal[A] => Traversal[B])
                    (implicit behaviourBuilder: RepeatBehaviour.Builder[B] => RepeatBehaviour.Builder[B] = RepeatBehaviour.noop[B] _)
    : Traversal[B] = {
    val behaviour = behaviourBuilder(new RepeatBehaviour.Builder[B]).build
    val _repeatTraversal = repeatTraversal.asInstanceOf[Traversal[B] => Traversal[B]] //this cast usually :tm: safe, because `B` is a supertype of `A`
    val repeat0: B => PathAwareTraversal[B] = PathAwareRepeatStep(_repeatTraversal, behaviour)
    new PathAwareTraversal(iterator.flatMap { a =>
      repeat0(a).elementsWithPath
    })
  }

  /** overriding to ensure that path tracking remains enabled after steps that instantiate new Traversals */
  override protected def mapElements[B](f: A => B): Traversal[B] =
    new PathAwareTraversal(elementsWithPathIterator.map { case (a, path) => (f(a), path) })
}

object PathAwareTraversal {
  def empty[A]: PathAwareTraversal[A] =
    new PathAwareTraversal(Iterator.empty)

  def fromSingle[A](a: A): PathAwareTraversal[A] =
    new PathAwareTraversal(Iterator.single((a, Vector.empty)))

  def from[A](iterable: IterableOnce[A]): PathAwareTraversal[A] =
    iterable match {
      case traversal: PathAwareTraversal[_] => traversal.asInstanceOf[PathAwareTraversal[A]]
      case traversal: Traversal[_] => traversal.asInstanceOf[Traversal[A]].enablePathTracking
      case iterable => new PathAwareTraversal[A](iterable.iterator.map(a => (a, Vector.empty)))
    }
}
