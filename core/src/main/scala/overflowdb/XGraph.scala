package overflowdb

import java.util
import java.util.Optional
import scala.collection.mutable
import scala.jdk.CollectionConverters.{IterableHasAsJava, IteratorHasAsJava, IteratorHasAsScala}

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


  def edgeJoin(oldQty: Array[Int], oldVal: Array[Object], newVal:Array[Object], newQty:Array[Int], newEdges: Array[BatchedUpdate.CreateEdge]): Unit ={
    var newEdge_i = 0
    var old_i = 0
    

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

  override def property(key: String): AnyRef = {
    val eid = g.schema.getPropertyIdByLabel(eid)
    val pos = (g.schema.getNKinds * eid + kindId) * 2
    val resItems = g.getStuffMulti(pos, seqId, g._properties)
    //if there is formally NONE, ONE or MAYBE items then we need to unwrap
    g.schema.getFormalQtyAtKindAndProperty(kindId, eid) match {
      case FormalQty.MANY => resItems
      case FormalQty.NONE => null
      case FormalQty.ONE => resItems.head
      case FormalQty.MAYBE => resItems.headOption.getOrElse(null)
    }
  }

  override def property[A](key: PropertyKey[A]): A = property(key.name).asInstanceOf[A]

  override def propertyOption[A](key: PropertyKey[A]): Optional[A] = propertyOption(key.name).asInstanceOf[Optional[A]]

  override def propertyOption(key: String): Optional[AnyRef] = Optional.ofNullable(property(key))

  override def propertiesMap(): util.Map[String, AnyRef] = {
    val res = new java.util.HashMap[String, Any]()
    for(eid <- g.schema.getPropertiesAtKind()){
      val key = g.schema.getLabelByPropertyId(eid)
      val pos = ( eid * g.schema.getNKinds + kindId) * 2
      val resItems = g.getStuffMulti(pos, seqId, g._properties)
      val value = g.schema.getFormalQtyAtKindAndProperty(kindId, eid) match {
        case FormalQty.MANY => resItems
        case FormalQty.NONE => null
        case FormalQty.ONE => resItems.head
        case FormalQty.MAYBE => resItems.headOption.getOrElse(null)
      }
      if(value != null) res.put(key, value)
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
      case FormalQty.ONE                => (seq, 1)
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
