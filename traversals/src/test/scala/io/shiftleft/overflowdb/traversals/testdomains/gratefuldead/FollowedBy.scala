package io.shiftleft.overflowdb.traversals.testdomains.gratefuldead

import io.shiftleft.overflowdb._
import scala.jdk.CollectionConverters._

object FollowedBy {
  val Label = "followedBy"
  object Properties {
    val Weight = "weight"

    val all: Set[String] = Set(Weight)
    val allAsJava: java.util.Set[String] = all.asJava
  }
  val layoutInformation = new EdgeLayoutInformation(Label, Properties.allAsJava)
  var factory: EdgeFactory[FollowedBy] = new EdgeFactory[FollowedBy] {
    override def forLabel(): String = FollowedBy.Label

    override def createEdge(graph: OdbGraph, outNode: NodeRef[OdbNode], inNode: NodeRef[OdbNode]): FollowedBy =
      new FollowedBy(graph, outNode.asInstanceOf[NodeRef[SongDb]], inNode.asInstanceOf[NodeRef[SongDb]])
  }
}

class FollowedBy(graph: OdbGraph, outVertex: NodeRef[SongDb], inVertex: NodeRef[SongDb])
  extends OdbEdge(graph, FollowedBy.Label, outVertex, inVertex, FollowedBy.Properties.allAsJava)
