package overflowdb.misc

class ArrayIter[+T <: AnyRef](items: Array[AnyRef], private var pos: Int, until: Int, stride: Int)
    extends scala.collection.Iterator[T] {
  override def hasNext: Boolean = {
    while (pos < until && items(pos) == null) pos += stride
    pos < until
  }

  override def next(): T =
    if (!hasNext) Iterator.empty[T].next() else { val res = items(pos); pos += stride; res.asInstanceOf[T] }
}
