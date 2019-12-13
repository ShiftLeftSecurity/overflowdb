package io.shiftleft.overflowdb.traversal

/**
 *
 */
sealed trait RepeatBehaviour[A] {

  /** technically only `def emit(A): Boolean` is required, but this is intended to help performance by avoiding function calls using local fields
   * TODO check if that's the case - alternative use a mutable object rather than a trait
   *  */
//  private[traversal] val hasConditionalEmitBehaviour: Boolean
  private[traversal] val emitEverything: Boolean
//  def emit(a: A): Boolean
}

object RepeatBehaviour {
  class Builder[A] {
    private var _emitEverything = false

    private var emitCondition: A => Boolean =
      _ => false //by default, emit nothing

    /* change default emit behaviour (emit nothing) to 'emit everything along the way' */
    def emit: Builder[A] = {
      _emitEverything = true
      emitCondition = _ => true
      this
    }

    private[traversal] def build: RepeatBehaviour[A] = new RepeatBehaviour[A] {
//      override def emit(a: A): Boolean = emitCondition(a)
      override private[traversal] val emitEverything = _emitEverything
    }
  }

}
