package overflowdb.traversal.testdomains.simple

import overflowdb._
import scala.jdk.CollectionConverters._

class Connection(graph: Graph, outVertex: NodeRef[ThingDb], inVertex: NodeRef[ThingDb])
  extends Edge(graph, Connection.Label, outVertex, inVertex, Connection.PropertyNames.all.asJava)

object Connection {
  val Label = "connection"

  object Properties {
    val Distance = PropertyKey[Int](PropertyNames.Distance)
    val Name = PropertyKey[String](PropertyNames.Name)
  }

  object PropertyNames {
    val Distance = "distance"
    val Name = "name"
    val all: Set[String] = Set(Distance, Name)
  }

  val layoutInformation = new EdgeLayoutInformation(Label, PropertyNames.all.asJava)

  var factory: EdgeFactory[Connection] = new EdgeFactory[Connection] {
    override def forLabel(): String = Connection.Label

    override def createEdge(graph: Graph, outNode: NodeRef[NodeDb], inNode: NodeRef[NodeDb]): Connection =
      new Connection(graph, outNode.asInstanceOf[NodeRef[ThingDb]], inNode.asInstanceOf[NodeRef[ThingDb]])
  }
}
