package overflowdb.traversal.testdomains.simple

import overflowdb._
import scala.jdk.CollectionConverters._

class Connection(graph: OdbGraph, outVertex: NodeRef[ThingDb], inVertex: NodeRef[ThingDb])
  extends OdbEdge(graph, Connection.Label, outVertex, inVertex, Connection.PropertyNames.all.asJava)

object Connection {
  val Label = "connection"

  object Properties {
    val Distance = PropertyKey[Int](PropertyNames.Distance)
  }

  object PropertyNames {
    val Distance = "distance"
    val all: Set[String] = Set(Distance)
  }

  val layoutInformation = new EdgeLayoutInformation(Label, PropertyNames.all.asJava)
  var factory: EdgeFactory[Connection] = new EdgeFactory[Connection] {
    override def forLabel(): String = Connection.Label

    override def createEdge(graph: OdbGraph, outNode: NodeRef[OdbNode], inNode: NodeRef[OdbNode]): Connection =
      new Connection(graph, outNode.asInstanceOf[NodeRef[ThingDb]], inNode.asInstanceOf[NodeRef[ThingDb]])
  }
}
