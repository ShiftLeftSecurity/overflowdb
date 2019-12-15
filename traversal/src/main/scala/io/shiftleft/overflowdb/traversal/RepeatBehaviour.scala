package io.shiftleft.overflowdb.traversal

trait RepeatBehaviour[A] { this: EmitBehaviour =>
  val untilCondition: Option[A => Boolean] = None
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

    private[traversal] def build: RepeatBehaviour[A] = {
      if (emitNothing) {
        new RepeatBehaviour[A] with EmitNothing {
          final override val untilCondition: Option[A => Boolean] = _untilCondition
        }
      } else if (emitEverything) {
        new RepeatBehaviour[A] with EmitEverything {
          final override val untilCondition: Option[A => Boolean] = _untilCondition
        }
      } else {
        new RepeatBehaviour[A] with EmitConditional[A] {
          final private val _emitCondition = emitCondition.get
          final override def emit(a: A): Boolean = _emitCondition(a)
          final override val untilCondition: Option[A => Boolean] = _untilCondition
        }
      }
    }
  }

}
