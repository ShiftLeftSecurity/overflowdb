//package overflowdb.traversal
//
//import overflowdb.Node

//trait RepeatBehaviour3[A] { this: EmitBehaviour =>
//}
//
//object RepeatBehaviour3 {
//
//  class Builder[A](val traversal: Traversal[A]) { this: TravRootTrait[A] =>
//    protected[this] var emitEverything: Boolean = false
//
//    /* configure `repeat` step to emit everything along the way */
//    def emit: Builder[A] = {
//      emitEverything = true
//      this
//    }
//
//    private[traversal] def build: RepeatBehaviour3[A] = {
//      if (emitEverything) {
//        new RepeatBehaviour3[A] with EmitEverything {
//        }
//      } else ???
//    }
//
//  }
//}
