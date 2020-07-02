package overflowdb.traversal

import RepeatBehaviour._

trait RepeatBehaviour[A] { this: EmitBehaviour =>
  val searchAlgorithm: SearchAlgorithm.Value
  val untilCondition: Option[A => Boolean]
  val times: Option[Int]

  def timesReached(currentDepth: Int): Boolean =
    times.isDefined && times.get <= currentDepth

  def untilConditionReached(element: A): Boolean =
    untilCondition.isDefined && untilCondition.get.apply(element)

  def shouldEmit(element: A, currentDepth: Int): Boolean
}

object RepeatBehaviour {
  sealed trait EmitBehaviour
  trait EmitNothing extends EmitBehaviour
  trait EmitAll extends EmitBehaviour
  trait EmitAllButFirst extends EmitBehaviour
  trait EmitConditional[A] extends EmitBehaviour

  object SearchAlgorithm extends Enumeration {
    type SearchAlgorithm = Value
    val DepthFirstSearch, BreadthFirstSearch = Value
  }

  def noop[A](builder: RepeatBehaviour.Builder[A]): Builder[A] = builder

  class Builder[A] {
    private[this] var _emitNothing: Boolean = true
    private[this] var _emitAll: Boolean = false
    private[this] var _emitAllButFirst: Boolean = false
    private[this] var _emitCondition: Option[A => Boolean] = None
    private[this] var _untilCondition: Option[A => Boolean] = None
    private[this] var _times: Option[Int] = None
    private[this] var _searchAlgorithm: SearchAlgorithm.Value = SearchAlgorithm.DepthFirstSearch

    /* configure repeat traversal to search "Breadth First", rather than the default "Breadth First" */
    def breadthFirstSearch: Builder[A] = {
      _searchAlgorithm = SearchAlgorithm.BreadthFirstSearch
      this
    }
    def bfs: Builder[A] = breadthFirstSearch

    /* configure `repeat` step to emit everything along the way */
    def emit: Builder[A] = {
      _emitNothing = false
      _emitAll = true
      _emitAllButFirst = false
      _emitCondition = Some(_ => true)
      this
    }

    /* configure `repeat` step to emit everything along the way, apart from the _first_ element */
    def emitAllButFirst: Builder[A] = {
      _emitNothing = false
      _emitAll = false
      _emitAllButFirst = true
      _emitCondition = Some(_ => true)
      this
    }

    /* configure `repeat` step to emit whatever meets the given condition */
    def emit(condition: A => Boolean): Builder[A] = {
      _emitNothing = false
      _emitAll = false
      _emitAllButFirst = false
      _emitCondition = Some(condition)
      this
    }

    /* configure `repeat` step to stop traversing when given condition is true */
    def until(condition: A => Boolean): Builder[A] = {
      _untilCondition = Some(condition)
      this
    }

    def times(value: Int): Builder[A] = {
      _times = Some(value)
      this
    }

    private[traversal] def build: RepeatBehaviour[A] = {
      if (_emitNothing) {
        new RepeatBehaviour[A] with EmitNothing {
          override val searchAlgorithm: SearchAlgorithm.Value = _searchAlgorithm
          override val untilCondition: Option[A => Boolean] = _untilCondition
          final override val times: Option[Int] = _times
          override def shouldEmit(element: A, currentDepth: Int): Boolean = false
        }
      } else if (_emitAll) {
        new RepeatBehaviour[A] with EmitAll {
          override val searchAlgorithm: SearchAlgorithm.Value = _searchAlgorithm
          override final val untilCondition: Option[A => Boolean] = _untilCondition
          final override val times: Option[Int] = _times
          override def shouldEmit(element: A, currentDepth: Int): Boolean = true
        }
      } else if (_emitAllButFirst) {
        new RepeatBehaviour[A] with EmitAllButFirst {
          override val searchAlgorithm: SearchAlgorithm.Value = _searchAlgorithm
          override final val untilCondition: Option[A => Boolean] = _untilCondition
          final override val times: Option[Int] = _times
          override def shouldEmit(element: A, currentDepth: Int): Boolean = currentDepth > 0
        }
      } else {
        val __emitCondition = _emitCondition
        new RepeatBehaviour[A] with EmitConditional[A] {
          override val searchAlgorithm: SearchAlgorithm.Value = _searchAlgorithm
          override final val untilCondition: Option[A => Boolean] = _untilCondition
          final private val _emitCondition = __emitCondition.get
          override final def shouldEmit(element: A, currentDepth: Int): Boolean = _emitCondition(element)
          final override val times: Option[Int] = _times
        }
      }
    }
  }

}
