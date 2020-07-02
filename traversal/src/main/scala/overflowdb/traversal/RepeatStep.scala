package overflowdb.traversal

import overflowdb.traversal.RepeatBehaviour.{EmitAll, EmitAllButFirst, EmitConditional, EmitNothing}

import scala.collection.{Iterator, mutable}

object RepeatStep {

  object DepthFirst {
    case class StackItem[A](traversal: Traversal[A], depth: Int)

    def apply[B](repeatTraversal: B => Traversal[B], behaviour: RepeatBehaviour[B]): B => Traversal[B] = {
      def emitMaybe(element: B, currentDepth: Int, emitSack: mutable.Queue[B]): Unit = {
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
                emitMaybe(element, depth, emitSack)
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
      def traversalConsideringEmit(results: List[B], currentDepth: Int, emitSack: mutable.ListBuffer[B]): List[B] = {
        behaviour match {
          case _: EmitNothing => results.flatMap(repeatTraversal)
          case _: EmitAll => results.flatMap { element => emitSack.addOne(element); repeatTraversal(element) }
          case _: EmitAllButFirst if currentDepth > 0 => results.flatMap { element => emitSack.addOne(element); repeatTraversal(element) }
          case _: EmitAllButFirst => results.flatMap(repeatTraversal)
          case condition: EmitConditional[B] @unchecked =>
            results.flatMap { element =>
              if (condition.emit(element)) emitSack.addOne(element)
              repeatTraversal(element) }
        }
      }

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
          traversalResults = traversalConsideringEmit(traversalResults, currentDepth, emitSack)
          currentDepth += 1
        }
        Traversal(traversalResults ++ emitSack)
      }
    }
  }

}
