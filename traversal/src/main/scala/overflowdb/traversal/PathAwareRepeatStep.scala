package overflowdb.traversal

import overflowdb.traversal.RepeatStep._
import overflowdb.traversal.RepeatBehaviour.SearchAlgorithm

import scala.collection.{mutable, Iterator}

object PathAwareRepeatStep {
  import RepeatStep._

  case class WorklistItem[A](traversal: Traversal[A], depth: Int)

  /** @see
   *   [[Traversal.repeat]] for a detailed overview
   *
   * Implementation note: using recursion results in nicer code, but uses the JVM stack, which only has enough space for ~10k steps. So
   * instead, this uses a programmatic Stack which is semantically identical. The RepeatTraversalTests cover this case.
   */
  def apply[A](repeatTraversal: Traversal[A] => Traversal[A], behaviour: RepeatBehaviour[A]): A => PathAwareTraversal[A] = { (element: A) =>
    new PathAwareTraversal[A](new Iterator[(A, Vector[Any])] {
      val visited                                   = mutable.Set.empty[A]
      val emitSack: mutable.Queue[(A, Vector[Any])] = mutable.Queue.empty
      val worklist: Worklist[WorklistItem[A]] = behaviour.searchAlgorithm match {
        case SearchAlgorithm.DepthFirst   => new LifoWorklist()
        case SearchAlgorithm.BreadthFirst => new FifoWorklist()
      }

      worklist.addItem(WorklistItem(new PathAwareTraversal(Iterator.single((element, Vector.empty))), 0))

      def hasNext: Boolean = {
        if (emitSack.isEmpty) {
          // this may add elements to the emit sack and/or modify the worklist
          traverseOnWorklist
        }
        emitSack.nonEmpty || worklistTopHasNext
      }

      private def traverseOnWorklist: Unit = {
        var stop = false
        while (worklist.nonEmpty && !stop) {
          val WorklistItem(trav0, depth) = worklist.head
          val trav                       = trav0.asInstanceOf[PathAwareTraversal[A]].wrapped
          if (trav.isEmpty) worklist.removeHead()
          else if (behaviour.maxDepthReached(depth)) stop = true
          else {
            val (element, path1) = trav.next()
            if (behaviour.dedupEnabled) visited.addOne(element)

            if ( // `while/repeat` behaviour, i.e. check every time
              behaviour.whileConditionIsDefinedAndEmpty(element) ||
                // `repeat/until` behaviour, i.e. only checking the `until` condition from depth 1
                (depth > 0 && behaviour.untilConditionReached(element))
            ) {
              // we just consumed an element from the traversal, so in lieu adding to the emit sack
              emitSack.enqueue((element, path1))
              stop = true
            } else {
              val nextLevelTraversal = {
                val repeat =
                  repeatTraversal(new PathAwareTraversal(Iterator.single((element, path1))))
                if (behaviour.dedupEnabled) repeat.filterNot(visited.contains)
                else repeat
              }
              worklist.addItem(WorklistItem(nextLevelTraversal, depth + 1))

              if (behaviour.shouldEmit(element, depth))
                emitSack.enqueue((element, path1))

              if (emitSack.nonEmpty)
                stop = true
            }
          }
        }
      }

      private def worklistTopHasNext: Boolean =
        worklist.nonEmpty && worklist.head.traversal.hasNext

      override def next(): (A, Vector[Any]) = {
        val result = {
          if (emitSack.nonEmpty) emitSack.dequeue()
          else if (worklistTopHasNext) {
            worklist.head.traversal.asInstanceOf[PathAwareTraversal[A]].wrapped.next()
          } else throw new NoSuchElementException("next on empty iterator")
        }
        if (behaviour.dedupEnabled) visited.addOne(result._1)
        result
      }
    })
  }

}
