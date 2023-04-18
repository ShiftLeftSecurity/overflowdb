package overflowdb.traversal.testdomains.gratefuldead

import overflowdb._
import scala.jdk.CollectionConverters._

class SungBy(graph: Graph, outVertex: NodeRef[ArtistDb], inVertex: NodeRef[SongDb])
    extends Edge(graph, SungBy.Label, outVertex, inVertex, SungBy.PropertyNames.all.asJava)

object SungBy {
  val Label = "sungBy"

  object Properties {}

  object PropertyNames {
    val all: Set[String] = Set.empty
  }

  val layoutInformation = new EdgeLayoutInformation(Label, PropertyNames.all.asJava)

  var factory: EdgeFactory[SungBy] = new EdgeFactory[SungBy] {
    override def forLabel(): String = SungBy.Label

    override def createEdge(graph: Graph, outNode: NodeRef[NodeDb], inNode: NodeRef[NodeDb]): SungBy =
      new SungBy(graph, outNode.asInstanceOf[NodeRef[ArtistDb]], inNode.asInstanceOf[NodeRef[SongDb]])
  }
}
