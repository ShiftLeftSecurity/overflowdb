package io.shiftleft.overflowdb.traversal

sealed trait RepeatBehaviour[A] extends EmitBehaviour[A] with UntilBehaviour[A]

sealed trait EmitBehaviour[A] {
  def emit(a: A): Boolean
}

object EmitBehaviour {
  /* runtime optimization to avoid invoking [[EmitBehaviour.emit]] for every element
   * see [[RepeatBehaviour.Builder.build]]
   * see [[Traversal._repeat]] */
  sealed trait EmitNothing[A] extends EmitBehaviour[A] {
    final override def emit(a: A): Boolean = false
  }

  /* runtime optimization to avoid invoking [[EmitBehaviour.emit]] for every element
   * see [[RepeatBehaviour.Builder.build]]
   * see [[Traversal._repeat]] */
  sealed trait EmitEverything[A] extends EmitBehaviour[A] {
    final override def emit(a: A): Boolean = true
  }
}

sealed trait UntilBehaviour[A] {
  def until(a: A): Boolean
}

object UntilBehaviour {
  /* runtime optimization to avoid invoking [[UntilBehaviour.until]] for every element
   * see [[RepeatBehaviour.Builder.build]]
   * see [[Traversal._repeat]] */
  sealed trait NoUntilBehaviour[A] extends UntilBehaviour[A] {
    final override def until(a: A): Boolean = false
  }
}

object RepeatBehaviour {

  class Builder[A] {
    /* runtime optimization to avoid invoking [[EmitBehaviour.emit]] for every element */
    private[this] var emitNothing: Boolean = true
    /* runtime optimization to avoid invoking [[EmitBehaviour.emit]] for every element */
    private[this] var emitEverything: Boolean = false

    private[this] var emitCondition: A => Boolean =
      _ => false //by default, emit nothing
    private[this] var untilCondition: A => Boolean =
      _ => false //by default, do not stop anywhere

    /* configure `repeat` step to emit everything along the way */
    def emit: Builder[A] = {
      emitCondition = _ => true
      emitNothing = false
      emitEverything = true
      this
    }

    /* configure `repeat` step to emit whatever meets the given condition */
    def emit(condition: A => Boolean): Builder[A] = {
      emitNothing = false
      emitEverything = false
      emitCondition = condition
      this
    }

    /* configure `repeat` step to stop traversing when given condition is true */
    def until(condition: A => Boolean): Builder[A] = {
      untilCondition = condition
      this
    }

    private[traversal] def build: RepeatBehaviour[A] = {
      if (emitNothing) {
        new RepeatBehaviour[A] with EmitBehaviour.EmitNothing[A] {
          final override def until(a: A): Boolean = untilCondition(a)
        }
      } else if (emitEverything) {
        new RepeatBehaviour[A] with EmitBehaviour.EmitEverything[A] {
          final override def until(a: A): Boolean = untilCondition(a)
        }
      }

      new RepeatBehaviour[A] {
        final override def emit(a: A): Boolean = emitCondition(a)
        final override def until(a: A): Boolean = untilCondition(a)
      }
    }
  }

}
