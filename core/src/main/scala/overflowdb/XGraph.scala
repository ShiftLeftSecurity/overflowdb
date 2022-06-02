package overflowdb

import java.util
import java.util.Optional
import scala.collection.mutable
import scala.jdk.CollectionConverters.{IterableHasAsJava, IteratorHasAsJava, IteratorHasAsScala}

/*
abstract trait SortingAlg {
  protected def compareItemAtIdx(i:Int, j:Int):Int
  protected def swap(i:Int, j:Int):Unit
  protected def length:Int
  protected def sortInternal:Unit = {
    //fancy insertion sort
    val len = length
    var i = 1
    while(i < len){
      var j = i
      while(j > 0 && compareItemAtIdx(j-1, j) > 0){
        swap(j-1, j)
        j -= 1
      }
    }
    i += 1
  }
  def sort:Unit
}

final class SortByKey(keys:Array[Int], values:Array[Object]) extends SortingAlg {
  assert(keys.length ==values.length)
  override def swap(i: Int, j: Int): Unit = {
    val tmpkey = keys(i)
    val tmpval = values(i)
    keys(i) = keys(j)
    values(i)= values(j)
    keys(j) = tmpkey
    values(j) = tmpval
  }
  override def length: Int = keys.length
  override def compareItemAtIdx(i: Int, j: Int): Int = keys(i).compareTo(keys(j))
  override def sort:Unit = (sortInternal: @inline)
}
 */

class BatchupdateApplier {
  var newNodes: Array[mutable.ArrayBuffer[Object]] = null
  var outEdges: Array[mutable.ArrayBuffer[Object]] = null
  var inEdges: Array[mutable.ArrayBuffer[Object]] = null
  var properties: Array[mutable.ArrayBuffer[Object]] = null
  val nDelOutEdges: Array[Int] = null
  val nDelInEdges: Array[Int] = null

  /*join edge:
  old qty
  old values
  newedge
  delEdge
  delNode
   */

  /*join prop:
  old qty
  old values
  setProp
  delNode
  newNode
   */

  class ItemCopier[@specialized T] {

    /*Copies multi-valued objects
     * FIXME: Handle case where source-array is incomplete*/
    def itemCopyMultiMulti(oldQty: Array[Int],
                           oldVal: Object,
                           newVal: Object,
                           newQty: Array[Int],
                           fromInclusive: Int,
                           toExclusive: Int): Unit = {
      val start = oldQty(fromInclusive)
      val end = oldQty(toExclusive)
      val dstStart = newQty(fromInclusive)
      System.arraycopy(oldVal, start, newVal, dstStart, end - start)
      System.arraycopy(oldQty,
                       fromInclusive + 1,
                       newQty,
                       fromInclusive + 1,
                       toExclusive - fromInclusive)
      if (dstStart != start) {
        var i = fromInclusive + 1
        while (i <= toExclusive) {
          newQty(i) = newQty(i) + dstStart - start
          i += 1
        }
      }
    }

    /*Copies opt-valued objects
     * FIXME: Handle case where source-array is incomplete*/
    def itemCopyMaybeMaybe(oldQty: Array[Boolean],
                           oldVal: Object,
                           newVal: Object,
                           newQty: Array[Boolean],
                           fromInclusive: Int,
                           toExclusive: Int): Unit = {
      System.arraycopy(oldVal, fromInclusive, newVal, fromInclusive, toExclusive - fromInclusive)
      if (oldQty != null && newQty != null)
        System.arraycopy(oldQty, fromInclusive, newQty, fromInclusive, toExclusive - fromInclusive)
    }

    /*Copies opt-valued objects into multi-valued
     * FIXME: Handle case where source-array is incomplete
     *  FIXME: Use specialization*/
    def itemCopyMaybeMulti(oldQty: Array[Boolean],
                           oldVal: Array[T],
                           newVal: Array[T],
                           newQty: Array[Int],
                           fromInclusive: Int,
                           toExclusive: Int): Unit = {
      var i = fromInclusive
      val start = newQty(fromInclusive)
      var tot = 0
      while (i < toExclusive) {
        if (oldQty(i)) {
          newVal(start + tot) = oldVal(i)
          tot += 1
        }
        newQty(i + 1) = start + tot
        i += 1
      }
    }
  }
}

object FormalQty extends Enumeration {
  type FormalQty = Value
  val NONE, ONE, MAYBE, MANY = Value
}

class OdbSchema {
  def getNodeLabelByKind(kindId: Short): String = ???
  def getKindIdByLabel(label: String): Short = ???
  def getNKinds: Short = ???
  def getFactoryByKind(kindId: Short): Object = ???
  def getNFactoryByKind(kindId: Short): Object = ???

  def getPropertyIdByLabel(label: String): Short = ???
  def getLabelByPropertyId(id: Short): String = ???
  def getNProperties: Short = ???
  def getPropertiesAtKind(kind: Short): Array[Short] = ???
  def getFormalQtyAtKindAndProperty(kind: Short, eid: Short): FormalQty.FormalQty = ???
  def getPropertyKeysAtKind(kind: Short): util.Set[String] = ???

  def getEdgeIdByLabel(label: String): Short = ???
  def getLabelByEdgeId(id: Short): String = ???
  def getNEdges: Short = ???
  def getEdgeFactoryByEdgeId(id: Short): (XNode, XNode, Int) => Edge = ???

  def getOutEdgeIdsAtKind(kindId: Short): Array[Short] = ???
  def getInEdgeIdsAtKind(kindId: Short): Array[Short] = ???

}

/* fixme: handle invalid labels
 * */
class XNode(protected val g: XGraph, protected val kindId: Short, protected val seqId: Int)
    extends Node {
  override protected def addEdgeImpl(label: String, inNode: Node, keyValues: Any*): Edge = ???

  override protected def addEdgeImpl(label: String,
                                     inNode: Node,
                                     keyValues: util.Map[String, AnyRef]): Edge = ???

  override protected def addEdgeSilentImpl(label: String, inNode: Node, keyValues: Any*): Unit = ???

  override protected def addEdgeSilentImpl(label: String,
                                           inNode: Node,
                                           keyValues: util.Map[String, AnyRef]): Unit = ???

  override def id(): Long = ???

  override def out(): util.Iterator[Node] = {
    for (eid <- g.schema.getOutEdgeIdsAtKind(kindId).iterator;
         item <- g.getStuffMulti((eid * g.schema.getNKinds + kindId) * 4, seqId, g._edges))
      yield item.asInstanceOf[Node]
  }.asJava

  override def out(edgeLabels: String*): util.Iterator[Node] = {
    for (label <- edgeLabels.iterator;
         item <- g.getStuffMulti(
           (g.schema.getEdgeIdByLabel(label) * g.schema.getNKinds + kindId) * 4,
           seqId,
           g._edges)) yield item.asInstanceOf[Node]
  }.asJava

  override def in(): util.Iterator[Node] = {
    for (eid <- g.schema.getInEdgeIdsAtKind(kindId).iterator;
         item <- g.getStuffMulti((eid * g.schema.getNKinds + kindId) * 4 + 2, seqId, g._edges))
      yield item.asInstanceOf[Node]
  }.asJava

  override def in(edgeLabels: String*): util.Iterator[Node] = {
    for (label <- edgeLabels.iterator;
         item <- g.getStuffMulti(
           (g.schema.getEdgeIdByLabel(label) * g.schema.getNKinds + kindId) * 4 + 2,
           seqId,
           g._edges)) yield item.asInstanceOf[Node]
  }.asJava

  override def both(): util.Iterator[Node] = (in().asScala ++ out().asScala).asJava

  override def both(edgeLabels: String*): util.Iterator[Node] =
    (in(edgeLabels: _*).asScala ++ out(edgeLabels: _*).asScala).asJava

  override def outE(): util.Iterator[Edge] = {
    g.schema.getInEdgeIdsAtKind(kindId).iterator.flatMap { eid =>
      val factory = g.schema.getEdgeFactoryByEdgeId(eid)
      for ((neighbor, idx) <- g
             .getStuffMulti((eid * g.schema.getNKinds + kindId) * 4, seqId, g._edges)
             .zipWithIndex) yield factory(this, neighbor.asInstanceOf[XNode], idx + 1)
    }
  }.asJava

  override def outE(edgeLabels: String*): util.Iterator[Edge] = {
    edgeLabels.iterator.flatMap { edgeLabel =>
      val eid = g.schema.getEdgeIdByLabel(edgeLabel)
      val factory = g.schema.getEdgeFactoryByEdgeId(eid)
      for ((neighbor, idx) <- g
             .getStuffMulti((eid * g.schema.getNKinds + kindId) * 4, seqId, g._edges)
             .zipWithIndex) yield factory(this, neighbor.asInstanceOf[XNode], idx + 1)
    }
  }.asJava

  override def inE(): util.Iterator[Edge] = {
    g.schema.getOutEdgeIdsAtKind(kindId).iterator.flatMap { eid =>
      val factory = g.schema.getEdgeFactoryByEdgeId(eid)
      for ((neighbor, idx) <- g
             .getStuffMulti((eid * g.schema.getNKinds + kindId) * 4 + 2, seqId, g._edges)
             .zipWithIndex) yield factory(neighbor.asInstanceOf[XNode], this, -idx - 1)
    }
  }.asJava

  override def inE(edgeLabels: String*): util.Iterator[Edge] = {
    edgeLabels.iterator.flatMap { edgeLabel =>
      val eid = g.schema.getEdgeIdByLabel(edgeLabel)
      val factory = g.schema.getEdgeFactoryByEdgeId(eid)
      for ((neighbor, idx) <- g
             .getStuffMulti((eid * g.schema.getNKinds + kindId) * 4 + 2, seqId, g._edges)
             .zipWithIndex) yield factory(neighbor.asInstanceOf[XNode], this, -idx - 1)
    }
  }.asJava

  override def bothE(): util.Iterator[Edge] = (inE().asScala ++ outE().asScala).asJava

  override def bothE(edgeLabels: String*): util.Iterator[Edge] =
    (inE(edgeLabels: _*).asScala ++ outE(edgeLabels: _*).asScala).asJava

  override def label(): String = g.schema.getNodeLabelByKind(kindId)

  override def graph(): Graph = ???

  override def propertyKeys(): util.Set[String] = g.schema.getPropertyKeysAtKind(kindId)

  override def property(key: String): Object = {
    val eid = g.schema.getPropertyIdByLabel(key)
    val pos = (g.schema.getNKinds * eid + kindId) * 2
    val resItems = g.getStuffMulti(pos, seqId, g._properties)
    //if there is formally NONE, ONE or MAYBE items then we need to unwrap
    (g.schema.getFormalQtyAtKindAndProperty(kindId, eid) match {
      case FormalQty.MANY  => resItems
      case FormalQty.NONE  => null
      case FormalQty.ONE   => resItems.head
      case FormalQty.MAYBE => resItems.headOption.getOrElse(null)
    }).asInstanceOf[Object]
  }

  override def property[A](key: PropertyKey[A]): A = property(key.name).asInstanceOf[A]

  override def propertyOption[A](key: PropertyKey[A]): Optional[A] =
    propertyOption(key.name).asInstanceOf[Optional[A]]

  override def propertyOption(key: String): Optional[AnyRef] = Optional.ofNullable(property(key))

  override def propertiesMap(): util.Map[String, Object] = {
    val res = new java.util.HashMap[String, Object]()
    for (eid <- g.schema.getPropertiesAtKind(kindId)) {
      val key = g.schema.getLabelByPropertyId(eid)
      val pos = (eid * g.schema.getNKinds + kindId) * 2
      val resItems = g.getStuffMulti(pos, seqId, g._properties)
      val value = g.schema.getFormalQtyAtKindAndProperty(kindId, eid) match {
        case FormalQty.MANY  => resItems
        case FormalQty.NONE  => null
        case FormalQty.ONE   => resItems.head
        case FormalQty.MAYBE => resItems.headOption.getOrElse(null)
      }
      if (value != null) res.put(key, value.asInstanceOf[Object])
    }
    res
  }

  override protected def setPropertyImpl(key: String, value: Any): Unit = ???

  override protected def setPropertyImpl[A](key: PropertyKey[A], value: A): Unit = ???

  override protected def setPropertyImpl(property: Property[_]): Unit = ???

  override protected def removePropertyImpl(key: String): Unit = ???

  override protected def removeImpl(): Unit = ???
}

object ISeq {
  val empty = new ISeq(new Array[Nothing](0), 0, 0)

  def from(arr: Object, start: Int, end: Int): ISeq[Any] = {
    arr match {
      case null              => ISeq.empty
      case a: Array[Boolean] => new ISeq(a, start, end)
      case a: Array[Byte]    => new ISeq(a, start, end)
      case a: Array[Short]   => new ISeq(a, start, end)
      case a: Array[Int]     => new ISeq(a, start, end)
      case a: Array[Long]    => new ISeq(a, start, end)
      case a: Array[Float]   => new ISeq(a, start, end)
      case a: Array[Double]  => new ISeq(a, start, end)
      case a: Array[AnyRef]  => new ISeq(a, start, end)
    }
  }
}

class ISeq[@specialized +T](underlying: Array[T], start: Int, end: Int)
    extends scala.collection.immutable.IndexedSeq[T] {
  override def length: Int = end - start
  override def apply(idx: Int): T = underlying.apply(idx + start)
}

class XGraph(val schema: OdbSchema) {
  val _nodes: Array[Array[XNode]] = new Array(schema.getNKinds)
  val _properties: Array[Object] = new Array(2 * schema.getNKinds * schema.getNProperties)
  val _edges: Array[Object] = new Array(2 * 2 * schema.getNKinds * schema.getNEdges)
  val _edgeProperties: Array[Object] = new Array(2 * schema.getNKinds * schema.getNEdges)

  def getRange(pos: Int, seq: Int, stuff: Array[Object]): (Int, Int) = {
    stuff(pos) match {
      case null                  => (0, 0)
      case maybe: Array[Boolean] => if (maybe(seq)) (seq, 1) else (seq, 0)
      case range: Array[Int]     => (range(seq), range(seq + 1) - range(seq))
      case FormalQty.ONE         => (seq, 1)
    }
  }

  def getStuffMulti(pos: Int, seq: Int, stuff: Array[Object]): ISeq[Any] = {
    val (start, num) = getRange(pos, seq, stuff)
    if (num == 0) return ISeq.empty
    else ISeq.from(stuff(pos + 1), start, start + num)
  }

  def getStuffSingle(pos: Int, seq: Int, stuff: Array[Object]): Any = {
    val (start, num) = getRange(pos, seq, stuff)
    if (num != 1) return null
    else return stuff(pos + 1).asInstanceOf[Array[_]].apply(start)
  }

  def getStuffOpt(pos: Int, seq: Int, stuff: Array[Object]): Option[Any] = {
    val (start, num) = getRange(pos, seq, stuff)
    if (num != 1) return None
    else return Some(stuff(pos + 1).asInstanceOf[Array[_]].apply(start))
  }

}
