package io.shiftleft.overflowdb.traversals.testdomains.gratefuldead

import io.shiftleft.overflowdb.{NodeLayoutInformation, NodeRef, OdbNode}
import org.apache.tinkerpop.gremlin.structure.VertexProperty
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils
import io.shiftleft.overflowdb.OdbNodeProperty

class SongDb(ref: NodeRef[SongDb]) extends OdbNode(ref) {
  private var _name: String = null
  private var _songType: String = null
  private var _performances: Integer = null

  def name: String = _name
  def songType: String = _songType
  def performances: Integer = _performances

  override protected def layoutInformation = Song.layoutInformation

  override def valueMap = {
    val properties = new java.util.HashMap[String, Any]
    if (_name != null) properties.put(Song.Properties.Name, _name)
    if (_songType != null) properties.put(Song.Properties.SongType, _songType)
    if (_performances != null) properties.put(Song.Properties.Performances, _performances)
    properties
  }
  
  override protected def specificProperties[V](key: String) = 
    key match {
      case Song.Properties.Name if _name != null => 
        IteratorUtils.of(new OdbNodeProperty(this, key, _name.asInstanceOf[V]))
      case Song.Properties.SongType if _songType != null => 
        IteratorUtils.of(new OdbNodeProperty(this, key, _songType.asInstanceOf[V]))
      case Song.Properties.Performances if _performances != null => 
        IteratorUtils.of(new OdbNodeProperty(this, key, _performances.asInstanceOf[V]))
      case _ => 
        java.util.Collections.emptyIterator
    }

  override protected def updateSpecificProperty[V](cardinality: VertexProperty.Cardinality, key: String, value: V) = 
    key match {
      case Song.Properties.Name => 
        _name = value.asInstanceOf[String]
        property(Song.Properties.Name)
      case Song.Properties.SongType => 
        _songType = value.asInstanceOf[String]
        property(Song.Properties.SongType)
      case Song.Properties.Performances => 
        _performances = value.asInstanceOf[Integer]
        property(Song.Properties.Performances)
      case _ =>       
        throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }

  override protected def removeSpecificProperty(key: String) = 
    key match {
      case Song.Properties.Name => _name = null
      case Song.Properties.SongType => _songType = null
      case Song.Properties.Performances => _performances = null
      case _ =>       
        throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }
}
