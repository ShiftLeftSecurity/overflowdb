package overflowdb.traversal

import scala.collection.{Iterator, mutable}

object RepeatStep {

  object DepthFirst {
    case class StackItem[A](traversal: Traversal[A], depth: Int)

    def apply[B](repeatTraversal: B => Traversal[B], behaviour: RepeatBehaviour[B]): B => Traversal[B] = {
      element: B => Traversal(new Iterator[B] {
        val emitSack: mutable.Queue[B] = mutable.Queue.empty
        val stack: mutable.Stack[StackItem[B]] = mutable.Stack.empty
        val startTraversal = Traversal.fromSingle(element)
        stack.push(StackItem(startTraversal, 0))

        def hasNext: Boolean = {
          if (emitSack.isEmpty) {
            // this may add elements to the emit sack and/or modify the stack
            traverseOnStack
          }
          emitSack.nonEmpty || stackTopTraversalHasNext
        }

        private def traverseOnStack: Unit = {
          var stop = false
          while (stack.nonEmpty && !stop) {
            val StackItem(trav, depth) = stack.top
            if (trav.isEmpty) stack.pop()
            else if (behaviour.timesReached(depth)) stop = true
            else {
              val element = trav.next
              if (behaviour.untilConditionReached(element)) {
                // we just consumed an element from the traversal, so in lieu adding to the emit sack
                emitSack.enqueue(element)
                stop = true
              } else {
                stack.push(StackItem(repeatTraversal(element), depth + 1))
                if (behaviour.shouldEmit(element, depth)) emitSack.enqueue(element)
                if (emitSack.nonEmpty) stop = true
              }
            }
          }
        }

        private def stackTopTraversalHasNext: Boolean =
          stack.nonEmpty && stack.top.traversal.hasNext

        override def next: B = {
          if (emitSack.hasNext) emitSack.dequeue
          else if (hasNext) stack.top.traversal.next
          else throw new NoSuchElementException("next on empty iterator")
        }

      })
    }
  }

  object BreadthFirst {

    def apply[B](repeatTraversal: B => Traversal[B], behaviour: RepeatBehaviour[B]): B => Traversal[B] = {
      element: B => {
        val emitSack = mutable.ListBuffer.empty[B]
        // using an eagerly evaluated collection type is the key for making this 'breadth first'
        var traversalResults = List(element)
        var currentDepth = 0
        while (traversalResults.nonEmpty && !behaviour.timesReached(currentDepth)) {
          if (behaviour.untilCondition.isDefined) {
            traversalResults = traversalResults.filter { element =>
              val untilConditionReached = behaviour.untilCondition.get.apply(element)
              if (untilConditionReached) emitSack.addOne(element)
              !untilConditionReached
            }
          }

          traversalResults = traversalResults.flatMap { element =>
            if (behaviour.shouldEmit(element, currentDepth)) emitSack.addOne(element)
            repeatTraversal(element)
          }
          currentDepth += 1
        }
        Traversal(traversalResults ++ emitSack)
      }
    }
  }

}
