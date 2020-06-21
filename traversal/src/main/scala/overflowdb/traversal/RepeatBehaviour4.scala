package overflowdb.traversal

import overflowdb.Node

trait RepeatBehaviour4[A] { this: EmitBehaviour =>
}

object RepeatBehaviour4 {

  class Builder[A](val traversal: Traversal[A]) {
    protected[this] var emitEverything: Boolean = false

    /* configure `repeat` step to emit everything along the way */
    def emit: Builder[A] = {
      emitEverything = true
      this
    }

    private[traversal] def build: RepeatBehaviour4[A] = {
      if (emitEverything) {
        new RepeatBehaviour4[A] with EmitEverything {
        }
      } else ???
    }

  }
}
