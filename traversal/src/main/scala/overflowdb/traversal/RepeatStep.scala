package overflowdb.traversal

object RepeatStep {
  case class StackItem[A](traversal: Traversal[A], depth: Int)
}

class RepeatStep {
  import RepeatStep._

}

