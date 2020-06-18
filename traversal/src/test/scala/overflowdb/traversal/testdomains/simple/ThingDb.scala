package overflowdb.traversal.testdomains.simple

import org.apache.tinkerpop.gremlin.structure.VertexProperty
import overflowdb._
import overflowdb.traversal._

class ThingDb(ref: NodeRef[ThingDb]) extends OdbNode(ref) {
  private var _name: String = null
  private var _size: Integer = null

  def name: String = _name
  def size: Integer = _size

  /* Thing --- followedBy --- Thing */
  def followedBy: Traversal[Thing] = out(Connection.Label).toScalaAs[Thing]

  override def valueMap = {
    val properties = new java.util.HashMap[String, Any]
    if (_name != null) properties.put(Thing.PropertyNames.Name, _name)
    if (_size != null) properties.put(Thing.PropertyNames.Size, _size)
    properties
  }

  override protected def specificProperty2(key: String) =
    key match {
      case Thing.PropertyNames.Name => _name
      case Thing.PropertyNames.Size => _size
      case _ => null
    }

  override protected def updateSpecificProperty[V](cardinality: VertexProperty.Cardinality, key: String, value: V) =
    key match {
      case Thing.PropertyNames.Name =>
        _name = value.asInstanceOf[String]
        property(Thing.PropertyNames.Name)
      case Thing.PropertyNames.Size =>
        _size = value.asInstanceOf[Integer]
        property(Thing.PropertyNames.Size)
      case _ =>
        throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }

  override protected def removeSpecificProperty(key: String) =
    key match {
      case Thing.PropertyNames.Name => _name = null
      case Thing.PropertyNames.Size => _size = null
      case _ =>
        throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }

  override protected def layoutInformation = Thing.layoutInformation

  override def toString = s"ThingDb(id=${ref.id}, name=$name, size=$size)"
}
