package overflowdb.traversal

import overflowdb.Node

trait RepeatBehaviour2[A] { this: EmitBehaviour =>
}

object RepeatBehaviour2 {

  implicit class BuilderForNode[A <: Node](val builder: Builder[A]) {
    def out: Builder[Node] = {
      builder.copy(builder.traversal.out)
    }
  }

  class Builder[A](val traversal: Traversal[A]) {
    protected[this] var emitEverything: Boolean = false

    protected[traversal] def copy[B](newTrav: Traversal[B]): Builder[B] = {
      val _emitEverything = emitEverything
      new Builder[B](newTrav) {
        emitEverything = _emitEverything
      }
    }

    /* configure `repeat` step to emit everything along the way */
    def emit: Builder[A] = {
      emitEverything = true
      this
    }

    private[traversal] def build: RepeatBehaviour2[A] = {
      if (emitEverything) {
        new RepeatBehaviour2[A] with EmitEverything {
        }
      } else ???
    }

  }
}
