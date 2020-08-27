package overflowdb.traversal.testdomains.hierarchical

import overflowdb.{NodeRef, OdbNode}

class ElephantDb(ref: NodeRef[ElephantDb]) extends OdbNode(ref) with Animal {
  override def species = "Elephant"

  private var _name: String = null
  def name: String = _name

  override def valueMap = {
    val properties = new java.util.HashMap[String, Any]
    if (_name != null) properties.put(Elephant.PropertyNames.Name, _name)
    properties
  }

  override def property(key: String) =
    key match {
      case Elephant.PropertyNames.Name => _name
      case _ => null
    }

  override protected def updateSpecificProperty(key: String, value: Object) =
    key match {
      case Elephant.PropertyNames.Name =>
        _name = value.asInstanceOf[String]
      case _ =>
        throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }

  override protected def removeSpecificProperty(key: String) =
    key match {
      case Elephant.PropertyNames.Name => _name = null
      case _ =>
        throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }

  override protected def layoutInformation = Elephant.layoutInformation

  override def toString = s"ElephantDb(id=${ref.id}, name=$name)"
}
