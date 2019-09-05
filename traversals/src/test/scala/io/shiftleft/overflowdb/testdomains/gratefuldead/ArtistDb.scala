package io.shiftleft.overflowdb.testdomains.gratefuldead

import io.shiftleft.overflowdb.{NodeLayoutInformation, NodeRef, OdbNode}
import org.apache.tinkerpop.gremlin.structure.VertexProperty
import scala.jdk.CollectionConverters._
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils
import io.shiftleft.overflowdb.OdbNodeProperty

class ArtistDb(ref: NodeRef[ArtistDb]) extends OdbNode(ref) {
  private var _name: String = null
  def name: String = _name

  override protected def layoutInformation = Artist.layoutInformation

  override def valueMap = {
    val properties = new java.util.HashMap[String, Any]
    if (_name != null) properties.put(Artist.PropertyKeys.Name, _name)
    properties
  }
  
  override protected def specificProperties[V](key: String) = 
    key match {
      case Artist.PropertyKeys.Name if _name != null => 
        IteratorUtils.of(new OdbNodeProperty(this, key, _name.asInstanceOf[V]))
      case _ => 
        java.util.Collections.emptyIterator
    }

  override protected def updateSpecificProperty[V](cardinality: VertexProperty.Cardinality, key: String, value: V) = 
    key match {
      case Artist.PropertyKeys.Name => 
        _name = value.asInstanceOf[String]
        property(Artist.PropertyKeys.Name)
      case _ =>       
        throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }

  override protected def removeSpecificProperty(key: String) = 
    key match {
      case Artist.PropertyKeys.Name => 
        _name = null
      case _ =>       
        throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }
}
