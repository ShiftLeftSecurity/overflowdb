package io.shiftleft.overflowdb.traversal

/**
  * TODO denis check if it's faster to use a mutable class with members, to avoid virtual method calls
  * n.b. this may not actually make a difference since the jit may do the same for us
  *
  * technically only `def emit(A): Boolean` is required, but this is intended to help performance by avoiding function calls using local fields
  * TODO denis check if that's really any faster than just specifying 'emit'
  *
  * third alternative: instance
  */
sealed trait RepeatBehaviour[A] extends EmitBehaviour[A]

sealed trait EmitBehaviour[A] {
  def emit(a: A): Boolean
}

trait EmitNothing[A] extends EmitBehaviour[A] {
  override def emit(a: A): Boolean = false
}

trait EmitEverything[A] extends EmitBehaviour[A] {
  override def emit(a: A): Boolean = true
}

object RepeatBehaviour {

  class Builder[A] {
    private[this] var emitEverything = false
    private[this] var emitCondition: Option[A => Boolean] = None

    /* configure `repeat` step to emit everything along the way' */
    def emit: Builder[A] = {
      emitEverything = true
      this
    }

    private[traversal] def build: RepeatBehaviour[A] =
      if (emitEverything) {
        new RepeatBehaviour[A] with EmitEverything[A]
      } else
        emitCondition match {
          case None => new RepeatBehaviour[A] with EmitNothing[A]
          case Some(condition) =>
            new RepeatBehaviour[A] {
              override def emit(a: A): Boolean = condition(a)
            }
        }

  }

}
