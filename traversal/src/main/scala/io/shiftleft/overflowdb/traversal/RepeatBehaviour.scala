package io.shiftleft.overflowdb.traversal

sealed trait RepeatBehaviour[A] extends EmitBehaviour[A]

sealed trait EmitBehaviour[A] {
  def emit(a: A): Boolean
}

trait EmitNothing[A] extends EmitBehaviour[A] {
  final override def emit(a: A): Boolean = false
}

trait EmitEverything[A] extends EmitBehaviour[A] {
  final override def emit(a: A): Boolean = true
}

object RepeatBehaviour {

  class Builder[A] {
    private[this] var emitEverything = false
    private[this] var emitCondition: Option[A => Boolean] = None

    /* configure `repeat` step to emit everything along the way */
    def emit: Builder[A] = {
      emitEverything = true
      emitCondition = None
      this
    }

    /* configure `repeat` step to emit whatever meets the given condition */
    def emit(condition: A => Boolean): Builder[A] = {
      emitEverything = false
      emitCondition = Some(condition)
      this
    }

    private[traversal] def build: RepeatBehaviour[A] = {
      // technically we could create new instances every time, but using a single instance helps the JIT compiler to inline it
      if (emitEverything) {
        EmitEverything.asInstanceOf[RepeatBehaviour[A]]
      } else emitCondition match {
        case None => EmitNothing.asInstanceOf[RepeatBehaviour[A]]
        case Some(condition) => new RepeatBehaviour[A] { override def emit(a: A): Boolean = condition(a) }
      }
    }
    private val EmitEverything = new RepeatBehaviour[Any] with EmitEverything[Any]
    private val EmitNothing = new RepeatBehaviour[Any] with EmitEverything[Any]
  }

}
