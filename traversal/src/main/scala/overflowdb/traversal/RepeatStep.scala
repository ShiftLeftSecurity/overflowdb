package overflowdb.traversal

import overflowdb.traversal.RepeatBehaviour.SearchAlgorithm

import scala.collection.{Iterator, mutable}

object RepeatStep {

  /** @see [[Traversal.repeat]] for a detailed overview
   *
   * Implementation note: using recursion results in nicer code, but uses the JVM stack, which only has enough space
   * for ~10k steps. So instead, this uses a programmatic Stack which is semantically identical.
   * The RepeatTraversalTests cover this case.
   * */
  def apply[A](repeatTraversal: Traversal[A] => Traversal[A], behaviour: RepeatBehaviour[A]): A => Traversal[A] = {
    val worklist: Worklist[A] = behaviour.searchAlgorithm match {
      case SearchAlgorithm.DepthFirst   => new LifoWorklist[A]
      case SearchAlgorithm.BreadthFirst => new FifoWorklist[A]
    }

    element: A => {
      val res = new PathAwareTraversal[A](new Iterator[(A, Vector[Any])] {
        //      element: A => new PathAwareTraversal2[A](new Iterator[(A, Vector[Any])] {
        val emitSack: mutable.Queue[(A, Vector[Any])] = mutable.Queue.empty
        val startTraversal = Traversal.fromSingle(element)
        worklist.addItem(WorklistItem(startTraversal, 0, Vector.empty))

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
            val WorklistItem(trav, depth, path) = worklist.head
            if (trav.isEmpty) worklist.removeHead // TODO do we lose the L1/R1 path entries here?
            else if (behaviour.timesReached(depth)) stop = true
            else {
              val element = trav.next
              if (depth > 0  // `repeat/until` behaviour, i.e. only checking the `until` condition from depth 1
                && behaviour.untilConditionReached(element)) {
                // we just consumed an element from the traversal, so in lieu adding to the emit sack
                emitSack.enqueue((element, path))
                stop = true
              } else {
//                println(s"Repeat: adding $element to worklist") // L1/R1 are being added here, but lost later...
                worklist.addItem(WorklistItem(repeatTraversal(Traversal.fromSingle(element)), depth + 1, path.appended(element)))
                if (behaviour.shouldEmit(element, depth)) emitSack.enqueue((element, path))
                if (emitSack.nonEmpty) stop = true
              }
            }
          }
        }

        private def stackTopTraversalHasNext: Boolean =
          worklist.nonEmpty && worklist.head.traversal.hasNext

        override def next: (A, Vector[Any]) = {
          if (emitSack.hasNext) emitSack.dequeue
          else if (hasNext) {
            val res = (worklist.head.traversal.next, worklist.head.path)
//            println(s"Repeat.next: res.path=${res._2}") // looks correct...?
            res
          }
          else throw new NoSuchElementException("next on empty iterator")
        }

      })
//      println(s"xxx0: ${res.path.next}") // C, L1, L2 -> also correct. where does it break?
      res
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

  case class WorklistItem[A](traversal: Traversal[A], depth: Int, path: Vector[A])
}
