package overflowdb.traversal.testdomains.simple

import overflowdb._
import overflowdb.traversal._

class ThingDb(ref: NodeRef[ThingDb]) extends NodeDb(ref) {
  private var _name: String = null
  private var _size: Integer = null

  def name: String = _name
  def size: Integer = _size

  /* Thing --- followedBy --- Thing */
  def followedBy: Traversal[Thing] = out(Connection.Label).toScalaAs[Thing]

  override def property(key: String) =
    key match {
      case Thing.PropertyNames.Name => _name
      case Thing.PropertyNames.Size => _size
      case _                        => null
    }

  override protected def updateSpecificProperty(key: String, value: Object) =
    key match {
      case Thing.PropertyNames.Name =>
        _name = value.asInstanceOf[String]
      case Thing.PropertyNames.Size =>
        _size = value.asInstanceOf[Integer]
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

  override def layoutInformation = Thing.layoutInformation

  override def toString = s"ThingDb(id=${ref.id}, name=$name, size=$size)"
}
