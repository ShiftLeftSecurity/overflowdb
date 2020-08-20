// package overflowdb.traversal

// import overflowdb.traversal.RepeatBehaviour.SearchAlgorithm

// import scala.annotation.tailrec
// import scala.collection.{Iterator, mutable}

// object RepeatStep {

//   /** @see [[Traversal.repeat]] for a detailed overview
//    *
//    * Implementation note: using recursion results in nicer code, but uses the JVM stack, which only has enough space
//    * for ~10k steps. So instead, this uses a programmatic Stack which is semantically identical.
//    * The RepeatTraversalTests cover this case.
//    * */
//   def apply[A](repeatTraversal: Traversal[A] => Traversal[A], behaviour: RepeatBehaviour[A]): A => Traversal[A] = {
//     val worklist: Worklist[A] = behaviour.searchAlgorithm match {
//       case SearchAlgorithm.DepthFirst   => new LifoWorklist[A]
//       case SearchAlgorithm.BreadthFirst => new FifoWorklist[A]
//     }

//     element: A => Traversal(new Iterator[A] {
//       val visited = mutable.Set.empty[A]
//       var peekedElement: Option[A] = None // if defined, it already passes the dedupCheck
//       val emitSack: mutable.Queue[A] = mutable.Queue.empty
//       val startTraversal = Traversal.fromSingle(element)
//       worklist.addItem(WorklistItem(startTraversal, 0))

//       def hasNext: Boolean = {
//         peekedElement.isDefined || {
//           if (emitSack.isEmpty) {
//             // this may add elements to the emit sack and/or modify the stack
//             traverseOnStack
//           }
//           emitSack.nonEmpty || stackTopTraversalHasNext
//         }
//       }

//       private def traverseOnStack: Unit = {
//         var stop = false
//         while (worklist.nonEmpty && !stop) {
//           val WorklistItem(trav, depth) = worklist.head
//           if (trav.isEmpty) worklist.removeHead
//           else if (behaviour.timesReached(depth)) stop = true
//           else {
//             val element = trav.next
//             withDedupFilter(element) { () =>
// //              println(s"xxxx $element")
//               if (depth > 0  // `repeat/until` behaviour, i.e. only checking the `until` condition from depth 1
//                 && behaviour.untilConditionReached(element)) {
//                 // we just consumed an element from the traversal, and since we want to return it, add it to the emitSack
//                 emitSack.enqueue(element)
//                 stop = true
//               } else {
//                 worklist.addItem(WorklistItem(repeatTraversal(Traversal.fromSingle(element)), depth + 1))
//                 if (behaviour.shouldEmit(element, depth)) emitSack.enqueue(element)
//                 if (emitSack.nonEmpty) stop = true
//               }
//             }
//           }
//         }
//       }

//       /** assumes that stackTopTraversal is nonEmpty */
//       private def stackTopPeek: A = {
//         val res = peekedElement.getOrElse {
//           val res = stackTopTraversal.next
//           peekedElement = Some(res)
//           res
//         }
//         println(s"stackTopPeek: $res")
//         res
//       }

//       private def stackTopTraversal =
//         worklist.head.traversal

//       @tailrec
//       private def stackTopTraversalHasNext: Boolean = {
//         worklist.nonEmpty && stackTopTraversal.hasNext && {
//           if (behaviour.dedupEnabled) {
//             val element = stackTopPeek
//             if (visited.contains(element)) {
//               peekedElement = None
//               stackTopTraversalHasNext // ignore element, recurse (continues with other elements)
//             } else {
//               visited.addOne(element)
//               true
//             }
//           } else {
//             true // first conditions are sufficient
//           }
//         }
//       }

//       private def withDedupFilter(element: A)(fun: () => Any): Unit = {
//         if (behaviour.dedupEnabled && !visited.contains(element)) {
//           println(s"adding $element to `visited`")
//           visited.addOne(element)
//           fun() // first encounter of this element: continue
//         }
//       }

//       override def next: A = {
//         val res = {
//           if (peekedElement.isDefined) {
//             val res = peekedElement.get
//             println(s"taking $res from peekedElement and returning")
//             peekedElement = None
//             res
//           }
//           else if (emitSack.hasNext) emitSack.dequeue
//           else if (stackTopTraversalHasNext) stackTopTraversal.next
//           else throw new NoSuchElementException("next on empty iterator")
//         }
// //        if (behaviour.dedupEnabled) visited.addOne(res)
//         println(s"repeat.next: returning $res")
//         res
//       }

//     })
//   }

//   /** stores work still to do. depending on the underlying collection type, the behaviour of the repeat step changes */
//   trait Worklist[A] {
//     def addItem(item: WorklistItem[A]): Unit
//     def nonEmpty: Boolean
//     def head: WorklistItem[A]
//     def removeHead: Unit
//   }

//   /** stack based worklist for [[RepeatBehaviour.SearchAlgorithm.DepthFirst]] */
//   class LifoWorklist[A] extends Worklist[A] {
//     private val stack = mutable.Stack.empty[WorklistItem[A]]
//     override def addItem(item: WorklistItem[A]) = stack.push(item)
//     override def nonEmpty = stack.nonEmpty
//     override def head = stack.top
//     override def removeHead = stack.pop
//   }

//   /** queue based worklist for [[RepeatBehaviour.SearchAlgorithm.BreadthFirst]] */
//   class FifoWorklist[A] extends Worklist[A] {
//     private val queue = mutable.Queue.empty[WorklistItem[A]]
//     override def addItem(item: WorklistItem[A]) = queue.enqueue(item)
//     override def nonEmpty = queue.nonEmpty
//     override def head = queue.head
//     override def removeHead = queue.dequeue
//   }

//   case class WorklistItem[A](traversal: Traversal[A], depth: Int)
// }
