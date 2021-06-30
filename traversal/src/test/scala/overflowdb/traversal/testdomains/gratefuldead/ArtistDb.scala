package overflowdb.traversal.testdomains.gratefuldead

import overflowdb.traversal._
import overflowdb.{NodeRef, NodeDb}

class ArtistDb(ref: NodeRef[ArtistDb]) extends NodeDb(ref) {
  /* name property */
  def name: String = _name
  private var _name: String = null

  /* Artist <-- sungBy --- Song */
  def sangSongs: Traversal[Song] = in(SungBy.Label).toScalaAs[Song]

  /* Artist <-- writtenBy --- Song */
  def wroteSongs: Traversal[Song] = in(WrittenBy.Label).toScalaAs[Song]

  override def valueMap = {
    val properties = new java.util.HashMap[String, Any]
    if (_name != null) properties.put(Artist.PropertyNames.Name, _name)
    properties
  }
  
  override def property(key: String) =
    key match {
      case Artist.PropertyNames.Name => _name
      case _ => null
    }

  override protected def updateSpecificProperty(key: String, value: Object) =
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

  override def layoutInformation = Artist.layoutInformation
}
