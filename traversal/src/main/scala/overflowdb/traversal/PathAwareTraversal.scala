package overflowdb.traversal

import scala.collection.{IterableOnce, Iterator}

class PathAwareTraversal[A](val elementsWithPath: IterableOnce[(A, Vector[Any])])
  extends Traversal[A](elementsWithPath.map(_._1)) {

  override def flatMap[B](f: A => IterableOnce[B]): Traversal[B] =
    new PathAwareTraversal(
      elementsWithPath.iterator.flatMap { case (a, path) =>
        f(a).iterator.map(b => (b, path.appended(a)))
      }
    )

  override def map[B](f: A => B): Traversal[B] =
    new PathAwareTraversal(
      elementsWithPath.iterator.map { case (a, path) =>
        val b = f(a)
        (b, path.appended(a))
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
      elementsWithPath.iterator.filter(x => pred(x._1))
    )

  override def filterNot(pred: A => Boolean): Traversal[A] =
    new PathAwareTraversal(
      elementsWithPath.iterator.filterNot(x => pred(x._1))
    )

  override def dedup(implicit behaviourBuilder: DedupBehaviour.Builder => DedupBehaviour.Builder): Traversal[A] =
    new PathAwareTraversal(
      behaviourBuilder(new DedupBehaviour.Builder).build.comparisonStyle match {
        case DedupBehaviour.ComparisonStyle.HashAndEquals =>
          elementsWithPath.iterator.to(LazyList).distinctBy(_._1)
        case DedupBehaviour.ComparisonStyle.HashOnly =>
          elementsWithPath.iterator.to(LazyList).distinctBy(_._1.hashCode)
      }
    )

  // TODO add type safety once we're on dotty, similar to gremlin-scala's as/label steps with typelevel append?
  override def path: Traversal[Vector[Any]] =
    new Traversal(elementsWithPath.iterator.map {
      case (a, path) => path.appended(a)
    })

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
    new PathAwareTraversal(elementsWithPath.iterator.map { case (a, path) => (f(a), path) })
}

object PathAwareTraversal {
  def empty[A]: PathAwareTraversal[A] =
    new PathAwareTraversal(Iterator.empty)

  def fromSingle[A](a: A): PathAwareTraversal[A] =
    new PathAwareTraversal(Iterator.single((a, Vector.empty)))

  def from[A](iterable: IterableOnce[A]): PathAwareTraversal[A] =
    iterable match {
      case traversal: PathAwareTraversal[A] => traversal
      case traversal: Traversal[A] => traversal.enablePathTracking
      case iterable => new PathAwareTraversal[A](iterable.iterator.map(a => (a, Vector.empty)))
    }
}