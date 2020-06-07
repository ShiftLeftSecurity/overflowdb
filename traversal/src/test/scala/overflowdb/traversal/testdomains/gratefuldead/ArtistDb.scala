package overflowdb.traversal.testdomains.gratefuldead

import overflowdb.{NodeRef, OdbNode}
import org.apache.tinkerpop.gremlin.structure.{Direction, VertexProperty}

import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils
import overflowdb.OdbNodeProperty
import overflowdb.traversal.{NodeOps, Traversal}

class ArtistDb(ref: NodeRef[ArtistDb]) extends OdbNode(ref) with NodeOps {
  /* name property */
  def name: String = _name
  private var _name: String = null

  /* Artist <-- sungBy --- Song */
  def sangSongs: Traversal[Song] = adjacentNodes(Direction.IN, SungBy.Label)

  override def valueMap = {
    val properties = new java.util.HashMap[String, Any]
    if (_name != null) properties.put(Artist.PropertyNames.Name, _name)
    properties
  }
  
  override protected def specificProperties[V](key: String) = 
    key match {
      case Artist.PropertyNames.Name if _name != null =>
        IteratorUtils.of(new OdbNodeProperty(this, key, _name.asInstanceOf[V]))
      case _ => 
        java.util.Collections.emptyIterator
    }

  override protected def updateSpecificProperty[V](cardinality: VertexProperty.Cardinality, key: String, value: V) = 
    key match {
      case Artist.PropertyNames.Name =>
        _name = value.asInstanceOf[String]
        property(Artist.PropertyNames.Name)
      case _ =>       
        throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }

  override protected def removeSpecificProperty(key: String) = 
    key match {
      case Artist.PropertyNames.Name =>
        _name = null
      case _ =>       
        throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }

  override protected def layoutInformation = Artist.layoutInformation
}
