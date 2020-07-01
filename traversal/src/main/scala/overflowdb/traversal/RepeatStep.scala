package overflowdb.traversal

import overflowdb.traversal.RepeatBehaviour.{EmitAll, EmitAllButFirst, EmitConditional, EmitNothing}

import scala.collection.{Iterator, mutable}

object RepeatStep {

  object DepthFirst {
    case class StackItem[A](traversal: Traversal[A], depth: Int)

    def apply[B](repeatTraversal: B => Traversal[B], behaviour: RepeatBehaviour[B]): B => Traversal[B] = {
      def maybeAddToEmitSack(element: B, currentDepth: Int, emitSack: mutable.Queue[B]): Unit = {
        behaviour match {
          case _: EmitNothing =>
          case _: EmitAll => emitSack.enqueue(element)
          case _: EmitAllButFirst =>
            if (currentDepth > 0)
              emitSack.enqueue(element)
          case condition: EmitConditional[B]@unchecked =>
            if (condition.emit(element))
              emitSack.enqueue(element)
        }
      }

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
          while (stack.nonEmpty) {
            val StackItem(trav, depth) = stack.top
            if (trav.isEmpty) stack.pop()
            else if (behaviour.timesReached(depth)) return
            else {
              val element = trav.next
              if (behaviour.untilConditionReached(element)) {
                emitSack.enqueue(element)
                return
              } else {
                maybeAddToEmitSack(element, depth, emitSack)
                stack.push(StackItem(repeatTraversal(element), depth + 1))
                if (emitSack.nonEmpty) return
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

}

class RepeatStep {

}

