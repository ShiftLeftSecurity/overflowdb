package overflowdb.traversal

import RepeatBehaviour._

trait RepeatBehaviour[A] {
  val searchAlgorithm: SearchAlgorithm.Value
  val untilCondition: Option[A => Iterator[_]]
  val whileCondition: Option[A => Iterator[_]]
  val times: Option[Int]
  val dedupEnabled: Boolean

  def timesReached(currentDepth: Int): Boolean =
    times.isDefined && times.get <= currentDepth

  def untilConditionReached(element: A): Boolean =
    untilCondition match {
      case Some(untilConditionTraversal) => untilConditionTraversal(element).hasNext
      case None => false
    }

  def whileConditionIsDefinedAndEmpty(element: A): Boolean =
    whileCondition match {
      case Some(whileConditionTraversal) =>
        whileConditionTraversal(element).isEmpty
      case None =>
        false
    }

  def shouldEmit(element: A, currentDepth: Int): Boolean
}

object RepeatBehaviour {

  object SearchAlgorithm extends Enumeration {
    type SearchAlgorithm = Value
    val DepthFirst, BreadthFirst = Value
  }

  def noop[A](builder: RepeatBehaviour.Builder[A]): Builder[A] = builder

  class Builder[A] {
    private[this] var _emitNothing: Boolean = true
    private[this] var _emitAll: Boolean = false
    private[this] var _emitAllButFirst: Boolean = false
    private[this] var _emitCondition: Option[Traversal[A] => Traversal[_]] = None
    private[this] var _untilCondition: Option[Traversal[A] => Traversal[_]] = None
    private[this] var _whileCondition: Option[Traversal[A] => Traversal[_]] = None
    private[this] var _times: Option[Int] = None
    private[this] var _dedupEnabled: Boolean = false
    private[this] var _searchAlgorithm: SearchAlgorithm.Value = SearchAlgorithm.DepthFirst

    /* configure search algorithm to go "breadth first", rather than the default "depth first" */
    def breadthFirstSearch: Builder[A] = {
      _searchAlgorithm = SearchAlgorithm.BreadthFirst
      this
    }
    def bfs: Builder[A] = breadthFirstSearch

    /* configure `repeat` step to emit everything along the way */
    def emit: Builder[A] = {
      _emitNothing = false
      _emitAll = true
      _emitAllButFirst = false
      _emitCondition = Some(identity)
      this
    }

    /* configure `repeat` step to emit everything along the way, apart from the _first_ element */
    def emitAllButFirst: Builder[A] = {
      _emitNothing = false
      _emitAll = false
      _emitAllButFirst = true
      _emitCondition = Some(identity)
      this
    }

    /* configure `repeat` step to emit whatever meets the given condition */
    def emit(condition: Traversal[A] => Traversal[_]): Builder[A] = {
      _emitNothing = false
      _emitAll = false
      _emitAllButFirst = false
      _emitCondition = Some(condition)
      this
    }

    /* Configure `repeat` step to stop traversing when given condition-traversal has at least one result.
    * The condition-traversal is only evaluated _after_ the first iteration, for classic repeat/until behaviour */
    def until(condition: Traversal[A] => Traversal[_]): Builder[A] = {
      _untilCondition = Some(condition)
      this
    }

    /* Configure `repeat` step to stop traversing when given condition-traversal has at least one result.
    * The condition-traversal is already evaluated at the first iteration, for classic while/repeat behaviour.
    *
    * n.b. the only reason not to call this `while` is to avoid using scala keywords, which would need to be quoted. */
    def whilst(condition: Traversal[A] => Traversal[_]): Builder[A] = {
      _whileCondition = Some(condition)
      this
    }

    /* configure `repeat` step to perform the given amount of iterations */
    def times(value: Int): Builder[A] = {
      _times = Some(value)
      this
    }

    def dedup: Builder[A] = {
      _dedupEnabled = true
      this
    }

    private[traversal] def build: RepeatBehaviour[A] = {
      if (_emitNothing) {
        new RepeatBehaviour[A] {
          override val searchAlgorithm: SearchAlgorithm.Value = _searchAlgorithm
          override val untilCondition = _untilCondition.map(_.andThen(_.iterator).compose(Traversal.fromSingle))
          override val whileCondition = _whileCondition.map(_.andThen(_.iterator).compose(Traversal.fromSingle))
          final override val times: Option[Int] = _times
          final override val dedupEnabled = _dedupEnabled
          override def shouldEmit(element: A, currentDepth: Int): Boolean = false
        }
      } else if (_emitAll) {
        new RepeatBehaviour[A] {
          override val searchAlgorithm: SearchAlgorithm.Value = _searchAlgorithm
          override val untilCondition = _untilCondition.map(_.andThen(_.iterator).compose(Traversal.fromSingle))
          override val whileCondition = _whileCondition.map(_.andThen(_.iterator).compose(Traversal.fromSingle))
          final override val times: Option[Int] = _times
          final override val dedupEnabled = _dedupEnabled
          override def shouldEmit(element: A, currentDepth: Int): Boolean = true
        }
      } else if (_emitAllButFirst) {
        new RepeatBehaviour[A] {
          override val searchAlgorithm: SearchAlgorithm.Value = _searchAlgorithm
          override val untilCondition = _untilCondition.map(_.andThen(_.iterator).compose(Traversal.fromSingle))
          override val whileCondition = _whileCondition.map(_.andThen(_.iterator).compose(Traversal.fromSingle))
          final override val times: Option[Int] = _times
          final override val dedupEnabled = _dedupEnabled
          override def shouldEmit(element: A, currentDepth: Int): Boolean = currentDepth > 0
        }
      } else {
        val __emitCondition = _emitCondition
        new RepeatBehaviour[A] {
          override val searchAlgorithm: SearchAlgorithm.Value = _searchAlgorithm
          override val untilCondition = _untilCondition.map(_.andThen(_.iterator).compose(Traversal.fromSingle))
          override val whileCondition = _whileCondition.map(_.andThen(_.iterator).compose(Traversal.fromSingle))
          final private val _emitCondition = __emitCondition.get
          final override val times: Option[Int] = _times
          final override val dedupEnabled = _dedupEnabled
          override final def shouldEmit(element: A, currentDepth: Int): Boolean =
            _emitCondition(Traversal.fromSingle(element)).hasNext
        }
      }
    }
  }

}
