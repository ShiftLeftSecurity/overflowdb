package io.shiftleft.overflowdb.traversal.testdomains.gratefuldead

import io.shiftleft.overflowdb._
import scala.jdk.CollectionConverters._

class SungBy(graph: OdbGraph, outVertex: NodeRef[ArtistDb], inVertex: NodeRef[SongDb])
  extends OdbEdge(graph, SungBy.Label, outVertex, inVertex, SungBy.PropertyNames.all.asJava)

object SungBy {
  val Label = "sungBy"

  object Properties {}

  object PropertyNames {
    val all: Set[String] = Set.empty
  }

  val layoutInformation = new EdgeLayoutInformation(Label, PropertyNames.all.asJava)

  var factory: EdgeFactory[SungBy] = new EdgeFactory[SungBy] {
    override def forLabel(): String = SungBy.Label

    override def createEdge(graph: OdbGraph, outNode: NodeRef[OdbNode], inNode: NodeRef[OdbNode]): SungBy =
      new SungBy(graph, outNode.asInstanceOf[NodeRef[ArtistDb]], inNode.asInstanceOf[NodeRef[SongDb]])
  }
}