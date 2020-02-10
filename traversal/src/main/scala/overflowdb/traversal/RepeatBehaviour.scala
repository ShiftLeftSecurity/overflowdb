package overflowdb.traversal

trait RepeatBehaviour[A] { this: EmitBehaviour =>
  val untilCondition: Option[A => Boolean]
  val times: Option[Int]

  def timesReached(currentDepth: Int): Boolean =
    times.isDefined && times.get <= currentDepth
}

sealed trait EmitBehaviour
trait EmitNothing extends EmitBehaviour
trait EmitEverything extends EmitBehaviour
trait EmitConditional[A] extends EmitBehaviour {
  def emit(a: A): Boolean
}

object RepeatBehaviour {

  class Builder[A] {
    private[this] var emitNothing: Boolean = true
    private[this] var emitEverything: Boolean = false
    private[this] var emitCondition: Option[A => Boolean] = None
    private[this] var _untilCondition: Option[A => Boolean] = None
    private[this] var _times: Option[Int] = None

    /* configure `repeat` step to emit everything along the way */
    def emit: Builder[A] = {
      emitCondition = Some(_ => true)
      emitNothing = false
      emitEverything = true
      this
    }

    /* configure `repeat` step to emit whatever meets the given condition */
    def emit(condition: A => Boolean): Builder[A] = {
      emitCondition = Some(condition)
      emitNothing = false
      emitEverything = false
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
      if (emitNothing) {
        new RepeatBehaviour[A] with EmitNothing {
          override final val untilCondition: Option[A => Boolean] = _untilCondition
          final override val times: Option[Int] = _times
        }
      } else if (emitEverything) {
        new RepeatBehaviour[A] with EmitEverything {
          override final val untilCondition: Option[A => Boolean] = _untilCondition
          final override val times: Option[Int] = _times
        }
      } else {
        new RepeatBehaviour[A] with EmitConditional[A] {
          override final val untilCondition: Option[A => Boolean] = _untilCondition
          final private val _emitCondition = emitCondition.get
          override final def emit(a: A): Boolean = _emitCondition(a)
          final override val times: Option[Int] = _times
        }
      }
    }
  }

}
