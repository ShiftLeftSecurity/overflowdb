package overflowdb

import overflowdb.util.JIteratorCastingWrapper

package object traversal extends Implicits {

  implicit class NodeOps[N <: Node](val node: N) extends AnyVal {
    /** start a new Traversal with this Node, i.e. lift it into a Traversal */
    def start: Traversal[N] =
      Traversal.fromSingle(node)
  }

  implicit class JIterableOps[A](val jIterator: java.util.Iterator[A]) extends AnyVal {

    /**
      * Wraps a java iterator into a scala iterator, and casts it's elements.
      * This is faster than `jIterator.asScala.map(_.asInstanceOf[B])` because
      * 1) `.asScala` conversion is actually quite slow: multiple method calls and a `match` without @switch
      * 2) no additional `map` step that iterates and creates yet another iterator
     **/
    def toScalaAs[B]: IterableOnce[B] =
      new JIteratorCastingWrapper[B](jIterator)
  }

}
