package overflowdb.traversal

import overflowdb.{Edge, Element, Node}
import scala.jdk.CollectionConverters._

trait Implicits {
  implicit def jIteratortoTraversal[A](jiterator: java.util.Iterator[A]): Traversal[A] =
    iteratorToTraversal(jiterator.asScala)

  implicit def iteratorToTraversal[A](iterator: Iterator[A]): Traversal[A] =
    iterator.to(Traversal)

  implicit def iterableToTraversal[A](iterable: IterableOnce[A]): Traversal[A] =
    Traversal.from(iterable)

  implicit def toNodeTraversal[A <: Node](traversal: Traversal[A]): NodeTraversal[A] =
    new NodeTraversal[A](traversal)

  implicit def toEdgeTraversal[A <: Edge](traversal: Traversal[A]): EdgeTraversal[A] =
    new EdgeTraversal[A](traversal)

  implicit def toElementTraversal[A <: Element](traversal: Traversal[A]): ElementTraversal[A] =
    new ElementTraversal[A](traversal)

  // TODO make available again once we're on Scala 3.2.2
  // context: these break REPL autocompletion, e.g. in joern for `cpg.<tab>`
  // fixed via https://github.com/lampepfl/dotty/issues/16360#issuecomment-1324857836

  // implicit def toNodeTraversalViaAdditionalImplicit[A <: Node, Trav](traversable: Trav)(implicit toTraversal: Trav => Traversal[A]): NodeTraversal[A] =
  //   new NodeTraversal[A](toTraversal(traversable))

  // implicit def toEdgeTraversalViaAdditionalImplicit[A <: Edge, Trav](traversable: Trav)(implicit toTraversal: Trav => Traversal[A]): EdgeTraversal[A] =
  //   new EdgeTraversal[A](toTraversal(traversable))

  // implicit def toElementTraversalViaAdditionalImplicit[A <: Element, Trav](traversable: Trav)(implicit toTraversal: Trav => Traversal[A]): ElementTraversal[A] =
  //   new ElementTraversal[A](toTraversal(traversable))

  // implicit def toNumericTraversal[A : Numeric](traversal: Traversal[A]): NumericTraversal[A] =
  //   new NumericTraversal[A](traversal)

}

