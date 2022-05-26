package overflowdb.traversal

import overflowdb.Node

/**
 * Typeclass to prevent type inferencer to default to `Nothing` if no type parameter is given
 * used e.g. for `NodeTypeStarters:id`
 * */
sealed class DefaultsToNode[A]

object DefaultsToNode {

  implicit def overrideDefault[A]: DefaultsToNode[A] =
    new DefaultsToNode[A]

  implicit def default: DefaultsToNode[Node] =
    new DefaultsToNode[Node]

}

