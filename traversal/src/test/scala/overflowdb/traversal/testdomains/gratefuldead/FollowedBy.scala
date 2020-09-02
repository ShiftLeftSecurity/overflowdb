package overflowdb.traversal.testdomains.gratefuldead

import overflowdb._
import scala.jdk.CollectionConverters._

class FollowedBy(graph: Graph, outVertex: NodeRef[SongDb], inVertex: NodeRef[SongDb])
    extends Edge(graph, FollowedBy.Label, outVertex, inVertex, FollowedBy.PropertyNames.all.asJava) {
  def weight: Option[Integer] =
    Option(property(FollowedBy.Properties.Weight))
}

object FollowedBy {
  val Label = "followedBy"

  object Properties {
    val Weight = new PropertyKey[Int]("weight")
  }

  object PropertyNames {
    val Weight = "weight"
    val all: Set[String] = Set(Weight)
  }

  val layoutInformation = new EdgeLayoutInformation(Label, PropertyNames.all.asJava)
  var factory: EdgeFactory[FollowedBy] = new EdgeFactory[FollowedBy] {
    override def forLabel(): String = FollowedBy.Label

    override def createEdge(graph: Graph, outNode: NodeRef[NodeDb], inNode: NodeRef[NodeDb]): FollowedBy =
      new FollowedBy(graph, outNode.asInstanceOf[NodeRef[SongDb]], inNode.asInstanceOf[NodeRef[SongDb]])
  }
}
