package io.shiftleft.overflowdb.testdomains.gratefuldead

import io.shiftleft.overflowdb._
import scala.jdk.CollectionConverters._

object FollowedBy {
  val Label = "followedBy"
  object PropertyKeys {
    val Weight = "weight"

    val all: Set[String] = Set(Weight)
    val allAsJava: java.util.Set[String] = all.asJava
  }
  val layoutInformation = new EdgeLayoutInformation(Label, PropertyKeys.allAsJava)
  var factory: EdgeFactory[FollowedBy] = new EdgeFactory[FollowedBy] {
    override def forLabel(): String = FollowedBy.Label

    override def createEdge(graph: OdbGraph, outNode: NodeRef[OdbNode], inNode: NodeRef[OdbNode]): FollowedBy =
      new FollowedBy(graph, outNode, inNode)
  }
}

class FollowedBy(graph: OdbGraph, outVertex: NodeRef[_ <: OdbNode], inVertex: NodeRef[_ <: OdbNode])
  extends OdbEdge(graph, FollowedBy.Label, outVertex, inVertex, FollowedBy.PropertyKeys.allAsJava)
