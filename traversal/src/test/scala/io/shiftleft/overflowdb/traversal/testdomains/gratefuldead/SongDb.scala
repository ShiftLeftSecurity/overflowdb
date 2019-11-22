package io.shiftleft.overflowdb.traversal.testdomains.gratefuldead

import io.shiftleft.overflowdb.traversal.{NodeOps, Traversal}
import io.shiftleft.overflowdb.{NodeRef, OdbNode, OdbNodeProperty}
import org.apache.tinkerpop.gremlin.structure.{Direction, VertexProperty}
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils

class SongDb(ref: NodeRef[SongDb]) extends OdbNode(ref) with NodeOps {
  private var _name: String = null
  private var _songType: String = null
  private var _performances: Integer = null

  def name: String = _name
  def songType: String = _songType
  def performances: Integer = _performances

  /* Song --- followedBy --- Song */
  def followedBy: Traversal[Song] = adjacentNodes(Direction.OUT, FollowedBy.Label)

  override def valueMap = {
    val properties = new java.util.HashMap[String, Any]
    if (_name != null) properties.put(Song.PropertyNames.Name, _name)
    if (_songType != null) properties.put(Song.PropertyNames.SongType, _songType)
    if (_performances != null) properties.put(Song.PropertyNames.Performances, _performances)
    properties
  }

  override protected def specificProperties[V](key: String) =
    key match {
      case Song.PropertyNames.Name if _name != null =>
        IteratorUtils.of(new OdbNodeProperty(this, key, _name.asInstanceOf[V]))
      case Song.PropertyNames.SongType if _songType != null =>
        IteratorUtils.of(new OdbNodeProperty(this, key, _songType.asInstanceOf[V]))
      case Song.PropertyNames.Performances if _performances != null =>
        IteratorUtils.of(new OdbNodeProperty(this, key, _performances.asInstanceOf[V]))
      case _ =>
        java.util.Collections.emptyIterator
    }

  override protected def updateSpecificProperty[V](cardinality: VertexProperty.Cardinality, key: String, value: V) =
    key match {
      case Song.PropertyNames.Name =>
        _name = value.asInstanceOf[String]
        property(Song.PropertyNames.Name)
      case Song.PropertyNames.SongType =>
        _songType = value.asInstanceOf[String]
        property(Song.PropertyNames.SongType)
      case Song.PropertyNames.Performances =>
        _performances = value.asInstanceOf[Integer]
        property(Song.PropertyNames.Performances)
      case _ =>
        throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }

  override protected def removeSpecificProperty(key: String) =
    key match {
      case Song.PropertyNames.Name => _name = null
      case Song.PropertyNames.SongType => _songType = null
      case Song.PropertyNames.Performances => _performances = null
      case _ =>
        throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }

  override protected def layoutInformation = Song.layoutInformation
}
