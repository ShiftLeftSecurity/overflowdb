package overflowdb.traversal

import overflowdb.traversal.RepeatBehaviour.SearchAlgorithm
import overflowdb.traversal.RepeatStep.{FifoWorklist, LifoWorklist, Worklist, WorklistItem}

import scala.collection.{mutable, Iterator}
object RepeatStep {

  /** @see
    *   [[Traversal.repeat]] for a detailed overview
    *
    * Implementation note: using recursion results in nicer code, but uses the JVM stack, which only has enough space
    * for ~10k steps. So instead, this uses a programmatic Stack which is semantically identical. The
    * RepeatTraversalTests cover this case.
    */
  def apply[A](repeatTraversal: Traversal[A] => Traversal[A], behaviour: RepeatBehaviour[A]): A => Traversal[A] = {
    (element: A) =>
      new RepeatStepIterator[A](element, elem => repeatTraversal(Iterator.single(elem)), behaviour)
  }

  /** stores work still to do. depending on the underlying collection type, the behaviour of the repeat step changes */
  trait Worklist[A] {
    def addItem(item: A): Unit
    def nonEmpty: Boolean
    def head: A
    def removeHead(): Unit
  }

  /** stack based worklist for [[RepeatBehaviour.SearchAlgorithm.DepthFirst]] */
  class LifoWorklist[A] extends Worklist[A] {
    private val stack = mutable.Stack.empty[A]
    override def addItem(item: A) = stack.push(item)
    override def nonEmpty = stack.nonEmpty
    override def head = stack.top
    override def removeHead() = stack.pop()
  }

  /** queue based worklist for [[RepeatBehaviour.SearchAlgorithm.BreadthFirst]] */
  class FifoWorklist[A] extends Worklist[A] {
    private val queue = mutable.Queue.empty[A]
    override def addItem(item: A) = queue.enqueue(item)
    override def nonEmpty = queue.nonEmpty
    override def head = queue.head
    override def removeHead() = queue.dequeue()
  }

  case class WorklistItem[A](traversal: Iterator[A], depth: Int)
}

class RepeatStepIterator[A](element: A, repeatTraversal: A => Iterator[A], behaviour: RepeatBehaviour[A])
    extends Iterator[A] {
  import RepeatStep._
  val visited = mutable.Set.empty[A] // only used if dedup enabled
  val emitSack: mutable.Queue[A] = mutable.Queue.empty
  val worklist: Worklist[WorklistItem[A]] = behaviour.searchAlgorithm match {
    case SearchAlgorithm.DepthFirst   => new LifoWorklist()
    case SearchAlgorithm.BreadthFirst => new FifoWorklist()
  }

  worklist.addItem(WorklistItem(Iterator.single(element), 0))

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
      val WorklistItem(trav, depth) = worklist.head
      if (trav.isEmpty) worklist.removeHead()
      else if (behaviour.maxDepthReached(depth)) stop = true
      else {
        val element = trav.next()
        if (behaviour.dedupEnabled) visited.addOne(element)
        if ( // `while/repeat` behaviour, i.e. check every time
          behaviour.whileConditionIsDefinedAndEmpty(element) ||
          // `repeat/until` behaviour, i.e. only check the `until` condition from depth 1
          (depth > 0 && behaviour.untilConditionReached(element))
        ) {
          // we just consumed an element from the traversal, so in lieu adding to the emit sack
          emitSack.enqueue(element)
          stop = true
        } else {
          val nextLevelTraversal = {
            val repeat = repeatTraversal(element)
            if (behaviour.dedupEnabled) repeat.filterNot(visited.contains)
            else repeat
          }
          worklist.addItem(WorklistItem(nextLevelTraversal, depth + 1))
          if (behaviour.shouldEmit(element, depth)) emitSack.enqueue(element)
          if (emitSack.nonEmpty) stop = true
        }
      }
    }
  }

  private def worklistTopHasNext: Boolean =
    worklist.nonEmpty && worklist.head.traversal.hasNext

  override def next(): A = {
    val result = {
      if (emitSack.nonEmpty)
        emitSack.dequeue()
      else if (worklistTopHasNext)
        worklist.head.traversal.next()
      else throw new NoSuchElementException("next on empty iterator")
    }
    if (behaviour.dedupEnabled) visited.addOne(result)
    result
  }

}
