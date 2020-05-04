package io.shiftleft.overflowdb.traversal.testdomains.hierarchical

import io.shiftleft.overflowdb.traversal.{NodeOps, Traversal}
import io.shiftleft.overflowdb.{NodeRef, OdbNode, OdbNodeProperty}
import org.apache.tinkerpop.gremlin.structure.{Direction, VertexProperty}
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils

class ElephantDb(ref: NodeRef[ElephantDb]) extends OdbNode(ref) with Animal with NodeOps {
  override def species = "Elephant"

  private var _name: String = null
  def name: String = _name

  override def valueMap = {
    val properties = new java.util.HashMap[String, Any]
    if (_name != null) properties.put(Elephant.PropertyNames.Name, _name)
    properties
  }

  override protected def specificProperties[V](key: String) =
    key match {
      case Elephant.PropertyNames.Name if _name != null =>
        IteratorUtils.of(new OdbNodeProperty(this, key, _name.asInstanceOf[V]))
      case _ =>
        java.util.Collections.emptyIterator
    }

  override protected def updateSpecificProperty[V](cardinality: VertexProperty.Cardinality, key: String, value: V) =
    key match {
      case Elephant.PropertyNames.Name =>
        _name = value.asInstanceOf[String]
        property(Elephant.PropertyNames.Name)
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
