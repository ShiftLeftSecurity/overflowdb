package overflowdb.traversal

import overflowdb.{Edge, Element, Node}

// TODO drop this temporary helper once we're on Scala 3.3.0
object ChainedImplicitsTemp {
  implicit def toNodeTraversalViaAdditionalImplicit[A <: Node, Trav](traversable: Trav)(implicit
      toTraversal: Trav => Iterator[A]
  ): NodeTraversal[A] =
    new NodeTraversal[A](toTraversal(traversable))

  implicit def toEdgeTraversalViaAdditionalImplicit[A <: Edge, Trav](traversable: Trav)(implicit
      toTraversal: Trav => Iterator[A]
  ): EdgeTraversal[A] =
    new EdgeTraversal[A](toTraversal(traversable))

  implicit def toElementTraversalViaAdditionalImplicit[A <: Element, Trav](traversable: Trav)(implicit
      toTraversal: Trav => Iterator[A]
  ): ElementTraversal[A] =
    new ElementTraversal[A](toTraversal(traversable))

  implicit def toNumericTraversal[A: Numeric](traversal: Iterator[A]): NumericTraversal[A] =
    new NumericTraversal[A](traversal)
}
