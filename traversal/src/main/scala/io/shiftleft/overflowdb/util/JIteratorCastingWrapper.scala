package io.shiftleft.overflowdb.util

/** Wraps a java iterator into a scala iterator, and casts it's elements. */
class JIteratorCastingWrapper[A](underlying: java.util.Iterator[_]) extends Iterator[A] {
  def hasNext = underlying.hasNext
  def next(): A = underlying.next.asInstanceOf[A]
}
