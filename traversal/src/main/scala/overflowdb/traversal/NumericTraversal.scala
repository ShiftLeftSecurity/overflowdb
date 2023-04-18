package overflowdb.traversal

class NumericTraversal[N](val traversal: Iterator[N]) extends AnyVal {
  type Traversal[A] = Iterator[A]
  def greaterThan(n: N)(implicit numeric: Numeric[N]): Traversal[N] =
    traversal.filter(numeric.gt(_, n))

  def greaterThanEqual(n: N)(implicit numeric: Numeric[N]): Traversal[N] =
    traversal.filter(numeric.gteq(_, n))

  def lessThan(n: N)(implicit numeric: Numeric[N]): Traversal[N] =
    traversal.filter(numeric.lt(_, n))

  def lessThanEqual(n: N)(implicit numeric: Numeric[N]): Traversal[N] =
    traversal.filter(numeric.lteq(_, n))

  def equiv(n: N)(implicit numeric: Numeric[N]): Traversal[N] =
    traversal.filter(numeric.equiv(_, n))

  def between(startInclusive: N, endExclusive: N)(implicit numeric: Numeric[N]): Traversal[N] =
    traversal.filter(n => numeric.gteq(n, startInclusive) && numeric.lt(n, endExclusive))

  def inside(startExclusive: N, endExclusive: N)(implicit numeric: Numeric[N]): Traversal[N] =
    traversal.filter(n => numeric.gt(n, startExclusive) && numeric.lt(n, endExclusive))

  def outside(startInclusive: N, endInclusive: N)(implicit numeric: Numeric[N]): Traversal[N] =
    traversal.filter(n => numeric.lt(n, startInclusive) || numeric.gt(n, endInclusive))

}
