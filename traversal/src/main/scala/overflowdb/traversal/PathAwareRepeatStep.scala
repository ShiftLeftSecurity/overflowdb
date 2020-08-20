package overflowdb.traversal

import overflowdb.traversal.RepeatBehaviour.SearchAlgorithm

import scala.collection.{Iterator, mutable}

object PathAwareRepeatStep {

  /** @see [[Traversal.repeat]] for a detailed overview
   *
   * Implementation note: using recursion results in nicer code, but uses the JVM stack, which only has enough space
   * for ~10k steps. So instead, this uses a programmatic Stack which is semantically identical.
   * The RepeatTraversalTests cover this case.
   * */
  def apply[A](repeatTraversal: Traversal[A] => Traversal[A], behaviour: RepeatBehaviour[A]): A => PathAwareTraversal[A] = {
    val worklist: Worklist[A] = behaviour.searchAlgorithm match {
      case SearchAlgorithm.DepthFirst   => new LifoWorklist[A]
      case SearchAlgorithm.BreadthFirst => new FifoWorklist[A]
    }

    element: A => {
      new PathAwareTraversal[A](new Iterator[(A, Vector[Any])] {
        val visited = mutable.Set.empty[A]
        val emitSack: mutable.Queue[(A, Vector[Any])] = mutable.Queue.empty
        val startTraversal = PathAwareTraversal.fromSingle(element)
        worklist.addItem(WorklistItem(startTraversal, 0))

        def hasNext: Boolean = {
          if (emitSack.isEmpty) {
            // this may add elements to the emit sack and/or modify the stack
            traverseOnStack
          }
          emitSack.nonEmpty || stackTopTraversalHasNext
        }

        private def traverseOnStack: Unit = {
          var stop = false
          while (worklist.nonEmpty && !stop) {
            val WorklistItem(trav0, depth) = worklist.head
            val trav = trav0.path
            if (trav.isEmpty) worklist.removeHead
            else if (behaviour.timesReached(depth)) stop = true
            else {
              val path0 = trav.next
              val (path1, elementInSeq) = path0.splitAt(path0.size - 1)
              val element = elementInSeq.head.asInstanceOf[A]
//              withDedupFilter(element) { () =>
//                println("xxxxxxxx")
                visited.addOne(element)
                if (depth > 0  // `repeat/until` behaviour, i.e. only checking the `until` condition from depth 1
                  && behaviour.untilConditionReached(element)) {
                  // we just consumed an element from the traversal, so in lieu adding to the emit sack
                  emitSack.enqueue((element, path1))
                  stop = true
                } else {
                  val nextLevelTraversal =
                    repeatTraversal(new PathAwareTraversal(Iterator.single((element, path1))))
                  worklist.addItem(WorklistItem(nextLevelTraversal, depth + 1))

                  if (behaviour.shouldEmit(element, depth))
                    emitSack.enqueue((element, path1))

                  if (emitSack.nonEmpty)
                    stop = true
                }
//              }
            }
          }
        }

        private def withDedupFilter(element: A)(fun: () => Any): Unit = {
          if (behaviour.dedupEnabled && !visited.contains(element)) {
              visited.addOne(element)
              fun() // first encounter of this element: continue
          }
        }

        private def stackTopTraversalHasNext: Boolean =
//          worklist.nonEmpty && worklist.head.traversal.filterNot(visited.contains).hasNext
          worklist.nonEmpty && worklist.head.traversal.hasNext

        override def next: (A, Vector[Any]) = {
          if (emitSack.hasNext) emitSack.dequeue
          else if (stackTopTraversalHasNext) {
//            val entirePath = worklist.head.traversal.filterNot(visited.contains).path.next
            val entirePath = worklist.head.traversal.path.next
            val (path, lastElement) = entirePath.splitAt(entirePath.size - 1)
            (lastElement.head.asInstanceOf[A], path)
          }
          else throw new NoSuchElementException("next on empty iterator")
        }

      })
    }
  }

  /** stores work still to do. depending on the underlying collection type, the behaviour of the repeat step changes */
  trait Worklist[A] {
    def addItem(item: WorklistItem[A]): Unit
    def nonEmpty: Boolean
    def head: WorklistItem[A]
    def removeHead: Unit
  }

  /** stack based worklist for [[RepeatBehaviour.SearchAlgorithm.DepthFirst]] */
  class LifoWorklist[A] extends Worklist[A] {
    private val stack = mutable.Stack.empty[WorklistItem[A]]
    override def addItem(item: WorklistItem[A]) = stack.push(item)
    override def nonEmpty = stack.nonEmpty
    override def head = stack.top
    override def removeHead = stack.pop
  }

  /** queue based worklist for [[RepeatBehaviour.SearchAlgorithm.BreadthFirst]] */
  class FifoWorklist[A] extends Worklist[A] {
    private val queue = mutable.Queue.empty[WorklistItem[A]]
    override def addItem(item: WorklistItem[A]) = queue.enqueue(item)
    override def nonEmpty = queue.nonEmpty
    override def head = queue.head
    override def removeHead = queue.dequeue
  }

  case class WorklistItem[A](traversal: Traversal[A], depth: Int)
}
