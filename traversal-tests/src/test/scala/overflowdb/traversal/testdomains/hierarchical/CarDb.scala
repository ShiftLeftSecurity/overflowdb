package overflowdb.traversal.testdomains.hierarchical

import overflowdb.{NodeRef, NodeDb}

class CarDb(ref: NodeRef[CarDb]) extends NodeDb(ref) {
  private var _name: String = null

  def name: String = _name

  override def property(key: String) =
    key match {
      case Car.PropertyNames.Name => _name
      case _ => null
    }

  override protected def updateSpecificProperty(key: String, value: Object) =
    key match {
      case Car.PropertyNames.Name =>
        _name = value.asInstanceOf[String]
      case _ =>
        throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }

  override protected def removeSpecificProperty(key: String) =
    key match {
      case Car.PropertyNames.Name => _name = null
      case _ =>
        throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }

  override def layoutInformation = Car.layoutInformation

  override def toString = s"CarDb(id=${ref.id}, name=$name)"
}
