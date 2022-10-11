package overflowdb

import overflowdb.BatchedUpdate.DiffOrBuilder
import overflowdb.Disposition.Disposition
import overflowdb.FormalQty.FormalQty

import java.util
import java.util.Optional
import scala.collection.{mutable, ArrayOps}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.{
  IterableHasAsJava,
  IterableHasAsScala,
  IteratorHasAsJava,
  IteratorHasAsScala
}

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

/*
class SlowCopyProp[@specialized T](oldVal: Array[T],
                                   oldQty: Array[Int],
                                   newVal: Array[T],
                                   newQty: Array[Int],
                                   newNode: mutable.ArrayBuffer[(XNode, XNode)],
                                   delEdge: mutable.ArrayBuffer[(Int, Int)]) {}

class SlowCopyEdge(oldVal: Array[Object],
                   oldQty: Array[Int],
                   newVal: Array[Object],
                   newQty: Array[Int],
                   newEdge: mutable.ArrayBuffer[(XNode, XNode)],
                   delEdge: mutable.ArrayBuffer[(Int, Int)]) {}
 */
object FIXME {
  def copyEdgeOpt(src: Array[XNode],
                  nnodes: Int,
                  newEdges: mutable.ArrayBuffer[NewEdge]): (Array[Int], Array[XNode]) = {
    val dst = new Array[XNode](nnodes)
    System.arraycopy(src, 0, dst, 0, src.length)
    var overwrites = 0
    for (e <- newEdges) {
      if (dst(e.src.seqId) != null) overwrites += 1
      dst(e.src.seqId) = e.dst
    }
    if (overwrites == 0) return (null, dst)
    else {
      val dstLen = overwrites + dst.iterator.filter { _ != null }.length
      val dsti = new Array[Int](dst.length + 1)
      val dstnew = new Array[XNode](dstLen)

      val srci = new Array[Int](src.length + 1)
      val src2 = new Array[XNode](src.iterator.filter { _ != null }.length)
      var counter = 0
      for (idx <- Range(0, src.length)) {
        srci(idx) = counter
        if (src(idx) != null) {
          src2(counter) = src(idx)
          counter += 1
        }
      }
      srci(src.length) = counter
      copyEdgeMulti(src2, srci, nnodes, dstLen, newEdges)
    }
  }

  def copyEdgeMulti(src: Array[XNode],
                    srcQty: Array[Int],
                    nnodes: Int,
                    expectedSize: Int,
                    newEdges: mutable.ArrayBuffer[NewEdge]): (Array[Int], Array[XNode]) = {
    val dst = new Array[XNode](expectedSize)
    val dstQty = new Array[Int](nnodes)
    var nxIdx = 0
    for (e <- newEdges) {
      val seq = e.src.seqId
      acpy(src, srcQty, dst, dstQty, nxIdx, seq + 1)
      val insertpoint = dstQty(seq + 1)
      dst(insertpoint) = e.dst
      dstQty(seq + 1) = insertpoint + 1
      nxIdx = seq + 1
    }
    acpy(src, srcQty, dst, dstQty, nxIdx, dstQty.length - 1)
    (dstQty, dst)
  }

  def copyPropSingleDefault[T](src: Array[T],
                               nnodes: Int,
                               newProps: mutable.ArrayBuffer[SetProp],
                               default: T): (Array[_], Array[T]) = {
    val dst = java.lang.reflect.Array
      .newInstance(src.getClass.getComponentType, nnodes)
      .asInstanceOf[Array[T]]

    System.arraycopy(src, 0, dst, 0, src.length)
    for (idx <- Range(src.length, dst.length)) { dst(idx) = default }

    for (p <- newProps) {
      dst(p.node.seqId) = p.valusIter.nextOption().asInstanceOf[Option[T]].getOrElse(default)
    }
    (null, dst)
  }

  def copyPropSingle[T](src: Array[T],
                        srcqty: Array[Boolean],
                        nnodes: Int,
                        newProps: mutable.ArrayBuffer[SetProp]): (Array[Boolean], Array[T]) = {
    val dst = java.lang.reflect.Array
      .newInstance(src.getClass.getComponentType, nnodes)
      .asInstanceOf[Array[T]]
    val dstqty = new Array[Boolean](nnodes)
    //dst: Array[_],
    //dstqty: Array[Boolean],
    System.arraycopy(src, 0, dst, 0, src.length)
    System.arraycopy(srcqty, 0, dstqty, 0, src.length)

    val view = mutable.ArraySeq.make(dst)

    for (p <- newProps) {
      val idx = p.node.seqId
      p.valusIter.nextOption() match {
        case Some(v) =>
          view(idx) = v.asInstanceOf[T]
          dstqty(idx) = true
        case None =>
          dstqty(idx) = false
      }
    }
    (dstqty, dst)
  }

  def copyPropMulti[T](src: Array[T],
                       srcQty: Array[Int],
                       nnodes: Int,
                       expectedSize: Int,
                       newProps: mutable.ArrayBuffer[SetProp]): (Array[Int], Array[T]) = {
    val dst = java.lang.reflect.Array
      .newInstance(src.getClass.getComponentType, expectedSize)
      .asInstanceOf[Array[T]]
    val dstWrapped = mutable.ArraySeq.make(dst)
    val dstQty = new Array[Int](nnodes + 1)
    var nxIdx = 0
    for (p <- newProps) {
      val seq = p.node.seqId
      acpy(src, srcQty, dst, dstQty, nxIdx, seq)
      val insertpoint = dstQty(seq)
      var sz = 0
      for ((v, i) <- p.valusIter.zipWithIndex) {
        dstWrapped(insertpoint + i) = v.asInstanceOf[T]
        sz += 1
      }
      dstQty(seq + 1) = insertpoint + sz
      nxIdx = seq + 1
    }
    acpy(src, srcQty, dst, dstQty, nxIdx, nnodes)
    (dstQty, dst)
  }

  def acpy(src: AnyRef,
           srci: Array[Int],
           dst: AnyRef,
           dsti: Array[Int],
           from: Int,
           until: Int): Unit = {

    if (from <= until) return
    if (from > srci.length) {
      //there are no values, we just need to write the indices
      val lastpos = dsti(from)
      for (idx <- Range(from + 1, until + 1)) {
        dsti(idx) = lastpos
      }
      return
    }

    val until0 = scala.math.min(until, srci.length - 1)
    val sn = srci(from)
    val dn = dsti(from)
    val len = srci(until0 + 1) - sn
    System.arraycopy(src, sn, dst, dn, len)
    val off = dn - sn
    for (idx <- Range(from + 1, until0 + 1)) {
      dsti(idx) = srci(idx) + off
    }

    //we now need to set the remaining items
    if (until != until0) {
      val terminal = dsti(until0)
      for (idx <- Range(until0 + 1, until + 1)) {
        dsti(idx) = terminal
      }
    }
  }

}

/*
abstract class FIXME {
  def peekNewEdge(pos: Int): Int
  def copyUntil(fromIdx: Int, untilInclusive: Int): Unit
  def insertNew(pos: Int): Int
  def run(idxDone0: Int = -1, pos0: Int = 0): Unit = {
    var idxDone = idxDone0
    var posNext = pos0
    while (true) {
      val nxNew = peekNewEdge(nxNew)
      copyUntil(idxDone, nxNew)
      idxDone = nxNew
      posNext = insertNew(posNext)
      if (posNext == -1) return
    }
  }
}
class EdgeNoPropNullGuard(src: Array[XNode],
                          dst: Array[XNode],
                          newEdges: mutable.ArrayBuffer[NewEdge])
    extends FIXME {
  override def peekNewEdge(pos: Int): Int = newEdges(pos).src.seqId

  override def insertNew(pos: Int): Int = {
    val e = newEdges(pos)
    if (dst(e.src.seqId) != null || (pos + 1 < newEdges.size && newEdges(pos + 1).src.seqId == e.src.seqId)) {
      //fixme: recurse to multi
      return -1
    }
    dst(e.src.seqId) = e.dst
    pos + 1
  }

  override def copyUntil(fromIdx: Int, untilInclusive: Int): Unit = {
    val copylen = scala.math.min(untilInclusive - fromIdx, src.length)
    System.arraycopy(src, fromIdx + 1, dst, fromIdx + 1, copylen)
  }
}
class EdgeNoPropMulti(src: Array[XNode],
                      srcQty: Array[Int],
                      dst: Array[XNode],
                      dstQty: Array[Int],
                      newEdges: mutable.ArrayBuffer[NewEdge])
    extends FIXME {
  override def peekNewEdge(pos: Int): Int = newEdges(pos).src.seqId

  override def insertNew(pos: Int): Int = {
    val e = newEdges(pos)
    if (dst(e.src.seqId) != null || (pos + 1 < newEdges.size && newEdges(pos + 1).src.seqId == e.src.seqId)) {
      //fixme: recurse to multi
      return -1
    }
    dst(e.src.seqId) = e.dst
    pos + 1
  }

  override def copyUntil(fromIdx: Int, untilInclusive: Int): Unit = {
    val copylen = scala.math.min(untilInclusive - fromIdx, src.length)
    System.arraycopy(src, fromIdx + 1, dst, fromIdx + 1, copylen)
  }
}

abstract class EdgeCopy(newEdge: mutable.ArrayBuffer[(XNode, XNode)]) {
  def peek(ptr: Int): Int = {
    if (ptr < newEdge.size) newEdge(ptr)._1.seqId
    else -1
  }
  def copyFromOld(donePrev: Int, doneNext: Int): Unit = ???
  def copyFromNew(ptr: Int, active: Int): Int = ???

  def run(): Unit = {
    newEdge.sortInPlaceBy { _._1.seqId }
    var idx = 0
    var ptr = 0
    while (ptr >= 0) {
      val peekNext = peek(ptr)
      copyFromOld(idx, peekNext + 1)
      if (peekNext == -1) return //copyFromNew returns -1 if we had to recurse to update quantity
      ptr = copyFromNew(ptr, peekNext)
      idx = peekNext + 1
    }
  }
}

class SetProperty(val node: XNode, p: Any) {
  def getProp: Array[Object] = p match {
    case null               => null
    case arr: Array[Object] => arr
    case p: Object          => Array[Object](p)
  }
}
object Foocopy {

  def copyProperty(oldVal: Array[Object],
                   oldQty: Array[Int],
                   newVal: Array[Object],
                   newQty: Array[Int],
                   newNodeVals: mutable.ArrayBuffer[Any],
                   newNodeLens: Array[Int],
                   setProperties: mutable.ArrayBuffer[SetProperty]): Unit = {
    setProperties.sortInPlaceBy { _.node.seqId }
    var idx = 0
    var ptr = 0
    while (ptr >= 0) {
      if (ptr < setProperties.size) {
        val peekNext = setProperties(ptr).node.seqId
        copyMany2Many(oldVal, oldQty, newVal, newQty, idx, peekNext)
        val pos = newQty(peekNext)
        val arr = setProperties(ptr).getProp
        System.arraycopy(arr, 0, newVal, pos, arr.length)
        newQty(peekNext + 1) = pos + arr.length
        idx = peekNext + 1
        ptr += 1
      } else {
        copyMany2Many(oldVal, oldQty, newVal, newQty, idx, newQty.length - 1)
        ptr = -1
      }
    }
    //insert newnode properties
  }

  def copyEdges(oldVal: Array[Object],
                oldQty: Array[Int],
                newVal: Array[Object],
                newQty: Array[Int],
                newEdge: mutable.ArrayBuffer[(XNode, XNode)]): Unit = {
    newEdge.sortInPlaceBy { _._1.seqId }
    var idx = 0
    var ptr = 0
    while (ptr >= 0) {
      if (ptr < newEdge.size) {
        val peekNext = newEdge(ptr)._1.seqId
        copyMany2Many(oldVal, oldQty, newVal, newQty, idx, peekNext + 1)
        ptr = copyNew2Many(ptr, newVal, newQty, newEdge)
        idx = peekNext + 1
      } else {
        copyMany2Many(oldVal, oldQty, newVal, newQty, idx, newQty.length - 1)
        ptr = -1
      }
    }
  }

  def copyMany2Many[T <: AnyRef](oldVal: Array[T],
                                 oldQty: Array[Int],
                                 newVal: Array[T],
                                 newQty: Array[Int],
                                 fromInclusive: Int,
                                 untilExclusive: Int): Unit = {
    val oldQtyLast = oldQty.last
    val startOld = if (fromInclusive < oldQty.length) oldQty(fromInclusive) else oldQtyLast
    val endOld = if (untilExclusive < oldQty.length) oldQty(untilExclusive) else oldQtyLast
    System.arraycopy(oldVal, startOld, newVal, newQty(fromInclusive), endOld - startOld)

    val offset = newQty(fromInclusive) - startOld
    var idx = fromInclusive + 1
    val mayReadOldQty = math.min(untilExclusive + 1, oldQty.length)
    while (idx < mayReadOldQty) {
      newQty(idx) = oldQty(idx) + offset
      idx += 1
    }
    while (idx < untilExclusive + 1) {
      newQty(idx) = oldQtyLast + offset
      idx += 1
    }
  }
  def copyNew2Many(ptr: Int,
                   newVal: Array[Object],
                   newQty: Array[Int],
                   newEdge: mutable.ArrayBuffer[(XNode, XNode)]): Int = {
    val seq = newEdge(ptr)._1.seqId
    var off = 0
    val pos0 = newQty(seq + 1)
    while (true) {
      if (ptr + off >= newEdge.length) {
        newQty(seq + 1) = pos0 + off
        return ptr + off
      }
      val nxItem = newEdge(ptr + off)
      if (nxItem._1.seqId != seq) {
        newQty(seq + 1) = pos0 + off
        return ptr + off
      }
      newVal(pos0 + off) = nxItem._2
      off += 1
    }
    throw new RuntimeException("unreachable")
  }

}

class EdgeCopyManyToMany(oldVal: Array[XNode],
                         oldQty: Array[Int],
                         newVal: Array[XNode],
                         newQty: Array[Int],
                         newEdge: mutable.ArrayBuffer[(XNode, XNode)])
    extends EdgeCopy(newEdge) {
  override def copyFromOld(donePrev: Int, doneNext: Int): Unit = {}
}

abstract class ECMM(oldQty: Array[Int],
                    newQty: Array[Int],
                    newEdges: mutable.ArrayBuffer[BatchedUpdate.CreateEdge]) {
  var copiedTo: Int
  def upgradeToMany: Unit = ???
  def upgradeToMaybe: Unit = ???
  def copy(done: Int, toInclusive: Int): Unit = ???
  def unpack(ce: BatchedUpdate.CreateEdge): (Int, XNode) = ???

  def peek(value: Int): Int = ???
  def copyRest(done: Int): Unit = ???
  def run(): Unit = {
    var copied = -1
    var nxNE = 0
    while (true) {
      val nxE = 15
      if (nxE == -1) {
        copyRest(copied)
        return
      }
      copy(copied, nxE)
      copied = nxE
      while (peek(nxNE) != 0) null

    }
  }

}
 */
trait XChange {}
abstract class NewEdge() extends XChange {
  def label: String
  def eid: Int
  def src: XNode
  def dst: XNode
}
class DelNode extends XChange
class AddNode extends XChange
class SetProp(val node: XNode, val values: Any) extends XChange {
  def size: Int = values match {
    case null                          => 0
    case ar: Array[_]                  => ar.size
    case col: Iterable[_]              => col.size
    case jcol: java.util.Collection[_] => jcol.size()
    case jiter: java.lang.Iterable[_] =>
      var tmp = 0
      val iter = jiter.iterator()
      while (iter.hasNext) { tmp += 1; iter.next() }
      tmp
    case single => 1
  }
  def valusIter: Iterator[Any] =
    values match {
      case null                         => Iterator.empty
      case arr: Array[_]                => arr.iterator
      case col: Iterable[_]             => col.iterator
      case jiter: java.lang.Iterable[_] => jiter.asScala.iterator
      case _                            => Iterator.single(values)
    }
}
class NewNode extends XChange

class BatchupdateApplier(schema: OdbSchema, graph: XGraph) {
  var hasNodeDeletions = false
  var hasEdgeDeletions = false
  var newNodes: Array[mutable.ArrayBuffer[DetachedNodeData]] =
    new Array[mutable.ArrayBuffer[DetachedNodeData]](schema.getNKinds * schema.getNEdges)
  var edges: Array[mutable.ArrayBuffer[NewEdge]] =
    new Array[mutable.ArrayBuffer[NewEdge]](schema.getNKinds * schema.getNEdges * 2)
  var inDelEdges: Array[mutable.ArrayBuffer[Object]] =
    new Array[mutable.ArrayBuffer[Object]](schema.getNKinds * schema.getNEdges)
  var outDelEdges: Array[mutable.ArrayBuffer[Object]] =
    new Array[mutable.ArrayBuffer[Object]](schema.getNKinds * schema.getNEdges)

  var properties: Array[mutable.ArrayBuffer[SetProp]] =
    new Array[mutable.ArrayBuffer[Object]](schema.getNKinds * schema.getNProperties)
  val deltaProperties: Array[Int] = new Array[Int](schema.getNKinds * schema.getNProperties)
  val nDelNodes: Array[Int] = new Array[Int](schema.getNKinds)

  val deferred = mutable.ArrayDeque[DetachedNodeData]()

  def getXNode(nodeOrDetachedNode: NodeOrDetachedNode): XNode = {
    nodeOrDetachedNode match {
      case old: XNode => old
      case newNode: DetachedNodeData =>
        val nn = newNode.getRefOrId
        nn match {
          case null =>
            val eid = schema.getKindIdByLabel(newNode.label())
            val seq: Int = graph._nodes(eid).size + newNodes(eid) match {
              case null  => 0;
              case other => other.size
            }
            val xnode = schema.getNodeFactoryByKind(eid).create(graph, seq)
            emplace(newNodes, newNode, eid)
            newNode.setRefOrId(xnode)
            deferred.append(newNode)
            xnode
          case already: XNode => already
        }
    }
  }

  def drainDeferred(): Unit = {
    while (deferred.nonEmpty) {
      deferred.removeHead().recurseFields(this)
    }
  }

  def emplace[T](a: Array[mutable.ArrayBuffer[T]], item: T, pos: Int): Unit = {
    if (a(pos) == null) a(pos) = mutable.ArrayBuffer[T]()
    a(pos).append(item)
  }

  def processItems(items: Iterable[BatchedUpdate.Change]): Unit = {
    for (item <- items) {
      item match {
        case ce: BatchedUpdate.CreateEdge =>
          val eid = schema.getEdgeIdByLabel(ce.label)
          val src = getXNode(ce.src)
          val dst = getXNode(ce.dst)
          val factory = schema.getNewEdgeFactoryByEdgeId(eid)
          emplace(edges,
                  factory.create(src, dst, ce.propertiesAndKeys),
                  2 * (src.kindId + schema.getNKinds * eid))
          emplace(edges,
                  factory.create(dst, src, ce.propertiesAndKeys),
                  2 * (dst.kindId + schema.getNKinds * eid) + 1)
        case an: DetachedNodeData => getXNode(an)
        case set: BatchedUpdate.SetNodeProperty =>
          val node = set.node.asInstanceOf[XNode]
          val pid = schema.getPropertyIdByLabel(set.label)
          val prop = new SetProp(node, set.value)
        case delNode: BatchedUpdate.RemoveNode => ???
        /*{
          val xnode = delNode.node.asInstanceOf[XNode]
          nDelNodes(xnode.kindId) += 1
          XNode.delNode(xnode)
          hasNodeDeletions = true
        }*/
        case delEdge: BatchedUpdate.RemoveEdge => ???
      }
      drainDeferred()
    }
  }
  def rewrite(): Unit = {}
  def rewriteEdge(kid: Short, eid: Short): Option[(Array[Int], Array[XNode])] = {
    val nnodes = graph._nodes(kid).size + Option(newNodes(kid)).map { _.size }.getOrElse(0)
    Option(edges(kid + schema.getNKinds * eid)) match {
      case Some(buf) if buf.size != 0 =>
        val pos = (kid + schema.getNKinds * eid) * 4
        val qty = graph._edges(pos).asInstanceOf[Array[Int]]
        val neighbors = graph._edges(pos + 1).asInstanceOf[Array[XNode]]
        buf.sortInPlaceBy { _.src.seqId }
        if (qty == null && neighbors != null) {
          Some(FIXME.copyEdgeOpt(neighbors, nnodes, newEdges = buf))
        } else if (qty == null && neighbors == null) {
          Some(FIXME.copyEdgeOpt(new Array[XNode](0), nnodes, buf))
        } else {
          Some(FIXME.copyEdgeMulti(neighbors, qty, nnodes, qty.last + buf.size - 1, buf))
        }
      case _ => None
    }
  }

  def rewriteProperty(kid: Short, pid: Short): Unit = {
    if (newNodes(kid) == null && properties(kid + schema.getNKinds * pid) == null) return
    val helper = schema.getNewNodePropertyHelper(kid, pid)
    val setP = properties((kid + schema.getNKinds * pid) * 2)
    val pos = (kid + schema.getNProperties * pid)
    val oldqty = graph._properties(2 * pos)
    val oldVal = graph._properties(2 * pos + 1)
    val nns = Option(newNodes(kid)).getOrElse(ArrayBuffer.empty)
    val (nprop, multi) = helper.count(nns)

    //if(oldqty == null)

  }

  def sortFilterCount(a: mutable.ArrayBuffer[SetProp])
}

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
/*
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
 */
object FormalQty extends Enumeration {
  type FormalQty = Value
  val NONE, ONE, MAYBE, MANY = Value
}

object Disposition extends Enumeration {
  type Disposition = Value
  val PROPERTY, EDGE_IN, EDGE_OUT = Value
}

trait NodeFactory {
  def create(g: XGraph, seq: Int): XNode
}

trait NewEdgeFactory {
  def create(src: XNode, dst: XNode, props: Array[Object]): NewEdge = ???
}

trait NewNodePropertyHelper {
  def count(buf: Iterable[DetachedNodeData]): (Int, Int)
  def insert(dst: Array[_], dstQty: Array[_], newNodes: Iterable[DetachedNodeData]): Unit
  def defaultValue: Option[Any]
  def qtyBound: FormalQty.FormalQty
  def allocate(size: Int): Array[_]
}

class OdbSchema {
  def getNodeLabelByKind(kindId: Short): String = ???

  def getKindIdByLabel(label: String): Short = ???

  def getNKinds: Short = ???

  def getNodeFactoryByKind(kindId: Short): NodeFactory = ???

  def getNewNodeFactoryByKind(kindId: Short): Object = ???

  def getPropertyIdByLabel(label: String): Short = ???

  def getLabelByPropertyId(id: Short): String = ???

  def getNProperties: Short = ???

  def getPropertiesAtKind(kind: Short): Array[Short] = ???

  def getFormalQtyAtKindAndProperty(kind: Short, eid: Short): FormalQty.FormalQty = ???

  def getNewNodePropertyHelper(kind: Short, pid: Short): NewNodePropertyHelper = ???

  def getPropertyKeysAtKind(kind: Short): util.Set[String] = ???

  def getEdgeIdByLabel(label: String): Short = ???

  def getLabelByEdgeId(id: Short): String = ???

  def getNEdges: Short = ???

  def getEdgeFactoryByEdgeId(id: Short): (XNode, XNode, Int) => Edge = ???

  def getNewEdgeFactoryByEdgeId(id: Short): NewEdgeFactory = ???

  def getOutEdgeIdsAtKind(kindId: Short): Array[Short] = ???

  def getInEdgeIdsAtKind(kindId: Short): Array[Short] = ???

}

abstract class XEdge extends Edge {}

/* fixme: handle invalid labels
 * */
object XNode {
  def delNode(node: XNode): Unit = node.isDeleted = true

  def isDeleted(node: XNode) = node.isDeleted
}

class XNode(val g: XGraph, val kindId: Short, val seqId: Int) extends Node {
  private var isDeleted = false

  override protected def addEdgeImpl(label: String, inNode: Node, keyValues: Any*): Edge = ???

  override protected def addEdgeImpl(label: String,
                                     inNode: Node,
                                     keyValues: util.Map[String, AnyRef]): Edge = ???

  override protected def addEdgeSilentImpl(label: String, inNode: Node, keyValues: Any*): Unit = ???

  override protected def addEdgeSilentImpl(label: String,
                                           inNode: Node,
                                           keyValues: util.Map[String, AnyRef]): Unit = ???

  override def id(): Long = ???

  override def out(): util.Iterator[Node] =
    this.out(g.schema.getOutEdgeIdsAtKind(kindId).map(g.schema.getLabelByEdgeId): _*)

  override def out(edgeLabels: String*): util.Iterator[Node] = {
    for (label <- edgeLabels.iterator;
         item <- g
           ._edgeDescriptors(kindId + g.schema.getEdgeIdByLabel(label) * 2 * g.schema.getNKinds)
           .getMulti(seqId)) yield item.asInstanceOf[Node]
  }.asJava

  override def in(): util.Iterator[Node] =
    this.in(g.schema.getInEdgeIdsAtKind(kindId).map(g.schema.getLabelByEdgeId): _*)

  override def in(edgeLabels: String*): util.Iterator[Node] = {
    for (label <- edgeLabels.iterator;
         item <- g
           ._edgeDescriptors(
             kindId + (g.schema.getEdgeIdByLabel(label) * 2 + 1) * g.schema.getNKinds)
           .getMulti(seqId)) yield item.asInstanceOf[Node]
  }.asJava

  override def both(): util.Iterator[Node] = (in().asScala ++ out().asScala).asJava

  override def both(edgeLabels: String*): util.Iterator[Node] =
    (in(edgeLabels: _*).asScala ++ out(edgeLabels: _*).asScala).asJava

  override def outE(): util.Iterator[Edge] =
    this.outE(g.schema.getOutEdgeIdsAtKind(kindId).map(g.schema.getLabelByEdgeId): _*)

  override def outE(edgeLabels: String*): util.Iterator[Edge] = {
    edgeLabels.iterator.flatMap { edgeLabel =>
      val eid = g.schema.getEdgeIdByLabel(edgeLabel)
      val factory = g.schema.getEdgeFactoryByEdgeId(eid)
      for ((neighbor, idx) <- g
             ._edgeDescriptors(kindId + g.schema.getEdgeIdByLabel(label) * 2 * g.schema.getNKinds)
             .getMulti(seqId)
             .zipWithIndex) yield factory(this, neighbor.asInstanceOf[XNode], idx + 1)
    }
  }.asJava

  override def inE(): util.Iterator[Edge] =
    this.outE(g.schema.getInEdgeIdsAtKind(kindId).map(g.schema.getLabelByEdgeId): _*)

  override def inE(edgeLabels: String*): util.Iterator[Edge] = {
    edgeLabels.iterator.flatMap { edgeLabel =>
      val eid = g.schema.getEdgeIdByLabel(edgeLabel)
      val factory = g.schema.getEdgeFactoryByEdgeId(eid)
      for ((neighbor, idx) <- g
             ._edgeDescriptors(
               kindId + (g.schema.getEdgeIdByLabel(label) * 2 + 1) * g.schema.getNKinds)
             .getMulti(seqId)
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
    val pd = g._propertyDescriptors(kindId + eid * g.schema.getNKinds)
    if (pd == null) null
    else pd.get(seqId).asInstanceOf[AnyRef]
  }

  override def property[A](key: PropertyKey[A]): A = property(key.name).asInstanceOf[A]

  override def propertyOption[A](key: PropertyKey[A]): Optional[A] =
    propertyOption(key.name).asInstanceOf[Optional[A]]

  override def propertyOption(key: String): Optional[AnyRef] = Optional.ofNullable(property(key))

  override def propertiesMap(): util.Map[String, Object] = {
    val res = new java.util.HashMap[String, Object]()
    for (key <- g.schema.getPropertiesAtKind(kindId).map(g.schema.getLabelByPropertyId)) {
      val values = this.property(key)
      if (values != null) res.put(key, values)
    }
    res
  }

  override protected def setPropertyImpl(key: String, value: Any): Unit = ???

  override protected def setPropertyImpl[A](key: PropertyKey[A], value: A): Unit = ???

  override protected def setPropertyImpl(property: Property[_]): Unit = ???

  override protected def removePropertyImpl(key: String): Unit = ???

  override protected def removeImpl(): Unit = ???
}

class PropertyDescriptor(var formalQty: FormalQty,
                         var actualQty: FormalQty,
                         val nodeLabel: String,
                         val nodeId: Short,
                         val propertyName: String,
                         val propertyId: Short,
                         val disposition: Disposition,
                         val qty: AnyRef,
                         val values: AnyRef,
                         val schema: OdbSchema) {
  def get(seq: Int): Any = {
    formalQty match {
      case FormalQty.NONE  => null
      case FormalQty.ONE   => getSingle(seq)
      case FormalQty.MAYBE => getOpt(seq)
      case FormalQty.MANY  => getMulti(seq)
    }
  }
  def getSingle(seq: Int): Any = values.asInstanceOf[Array[_]](seq)
  def getOpt(seq: Int): Option[Any] = {
    actualQty match {
      case FormalQty.NONE => None
      case FormalQty.ONE  => Some(values.asInstanceOf[Array[_]](seq))
      case FormalQty.MANY => ???
      case FormalQty.MAYBE =>
        qty match {
          case null =>
            val vals = values.asInstanceOf[Array[_]]
            if (seq < vals.length) Option(vals(seq)) else None
          case asBool: Array[Boolean] =>
            if (seq < asBool.length && asBool(seq)) Some(values.asInstanceOf[Array[_]](seq))
            else None
        }
    }
  }
  def getMulti(seq: Int): ISeq[Any] = {
    actualQty match {
      case FormalQty.NONE => ISeq.empty
      case FormalQty.ONE  => ISeq.from(values, seq, seq + 1)
      case FormalQty.MAYBE =>
        qty match {
          case null =>
            val vals = values.asInstanceOf[Array[_]]
            if (seq < vals.length && vals(seq) != null) ISeq.from(vals, seq, seq + 1)
            else ISeq.empty
          case asBool: Array[Boolean] =>
            if (seq < asBool.length && asBool(seq)) ISeq.from(values, seq, seq + 1)
            else ISeq.from(values, seq, seq)
        }
      case FormalQty.MANY =>
        val ranges = qty.asInstanceOf[Array[Int]]
        if (seq + 1 < ranges.length) ISeq.from(values, ranges(seq), ranges(seq + 1))
        else ISeq.empty
    }

  }

  def checkInvariants(): Boolean = {
    (qty, values) match {
      case (null, null) =>
        if (actualQty != FormalQty.NONE) return false
      case (x, null) if x != null => return false
      case (x: Array[Boolean], _: Array[_]) if x != null =>
        if (actualQty != FormalQty.MAYBE) return false
      case (x: Array[Int], _) if x != null  => if (actualQty != FormalQty.MANY) return false
      case (null, x: Array[_]) if x != null =>
        //we cannot distinguish between mandatory ref-valued fields and optional null-guarded fields here.
        //That is OK for generated accessors who know formalQty. Generic accessors need to know that, though
        actualQty match {
          case FormalQty.ONE => // this is ok
          case FormalQty.MAYBE => //we have a nullguard
            if (!x.isInstanceOf[Array[AnyRef]]) return false
          case _ => return false
        }
      case _ => return false

    }

    true
  }
  def copy(): PropertyDescriptor = ???

}

object AccessorHelpers {}
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
  val _edgeDescriptors: Array[PropertyDescriptor] = new Array(
    2 * schema.getNKinds * schema.getNEdges)
  val _propertyDescriptors: Array[PropertyDescriptor] = new Array(
    2 * schema.getNKinds * schema.getNProperties)

  /*
  def getRange(pos: Int, seq: Int, stuff: Array[Object]): (Int, Int) = {
    stuff(pos) match {
      case null                  => (0, 0)
      case maybe: Array[Boolean] =>
        //fixme: handle case where seq is oob
        if (maybe(seq)) (seq, 1) else (seq, 0)
      case range: Array[Int] =>
        //fixme: handle case where seq is oob
        (range(seq), range(seq + 1) - range(seq))
      case FormalQty.ONE => (seq, 1)
    }
  }


  def getPropertyMulti(propId: Short, kindId: Short, seqId: Int): ISeq[Any] = ???

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
 */
}
