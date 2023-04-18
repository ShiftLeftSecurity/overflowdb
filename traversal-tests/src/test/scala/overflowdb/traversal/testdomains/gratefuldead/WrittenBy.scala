package overflowdb.traversal.testdomains.gratefuldead

import overflowdb._
import scala.jdk.CollectionConverters._

class WrittenBy(graph: Graph, outVertex: NodeRef[ArtistDb], inVertex: NodeRef[SongDb])
    extends Edge(graph, WrittenBy.Label, outVertex, inVertex, WrittenBy.PropertyNames.all.asJava)

object WrittenBy {
  val Label = "writtenBy"

  object Properties {}

  object PropertyNames {
    val all: Set[String] = Set.empty
  }

  val layoutInformation = new EdgeLayoutInformation(Label, PropertyNames.all.asJava)
  var factory: EdgeFactory[WrittenBy] = new EdgeFactory[WrittenBy] {
    override def forLabel(): String = WrittenBy.Label

    override def createEdge(graph: Graph, outNode: NodeRef[NodeDb], inNode: NodeRef[NodeDb]): WrittenBy =
      new WrittenBy(graph, outNode.asInstanceOf[NodeRef[ArtistDb]], inNode.asInstanceOf[NodeRef[SongDb]])
  }
}
