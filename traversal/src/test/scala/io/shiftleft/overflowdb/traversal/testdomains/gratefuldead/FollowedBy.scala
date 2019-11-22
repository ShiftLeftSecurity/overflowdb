package io.shiftleft.overflowdb.traversal.testdomains.gratefuldead

import io.shiftleft.overflowdb._
import scala.jdk.CollectionConverters._

class FollowedBy(graph: OdbGraph, outVertex: NodeRef[SongDb], inVertex: NodeRef[SongDb])
  extends OdbEdge(graph, FollowedBy.Label, outVertex, inVertex, FollowedBy.PropertyNames.all.asJava)

object FollowedBy {
  val Label = "followedBy"

  object Properties {
    val Weight = "weight"
  }

  object PropertyNames {
    val Weight = "weight"
    val all: Set[String] = Set(Weight)
  }

  val layoutInformation = new EdgeLayoutInformation(Label, PropertyNames.all.asJava)
  var factory: EdgeFactory[FollowedBy] = new EdgeFactory[FollowedBy] {
    override def forLabel(): String = FollowedBy.Label

    override def createEdge(graph: OdbGraph, outNode: NodeRef[OdbNode], inNode: NodeRef[OdbNode]): FollowedBy =
      new FollowedBy(graph, outNode.asInstanceOf[NodeRef[SongDb]], inNode.asInstanceOf[NodeRef[SongDb]])
  }
}