package io.shiftleft.overflowdb.testdomains.gratefuldead

import io.shiftleft.overflowdb._
import scala.jdk.CollectionConverters._

object WrittenBy {
  val Label = "writtenBy"
  object Properties {
    val all: Set[String] = Set.empty
    val allAsJava: java.util.Set[String] = all.asJava
  }
  val layoutInformation = new EdgeLayoutInformation(Label, Properties.allAsJava)
  var factory: EdgeFactory[WrittenBy] = new EdgeFactory[WrittenBy] {
    override def forLabel(): String = WrittenBy.Label

    override def createEdge(graph: OdbGraph, outNode: NodeRef[OdbNode], inNode: NodeRef[OdbNode]): WrittenBy =
      new WrittenBy(graph, outNode.asInstanceOf[NodeRef[ArtistDb]], inNode.asInstanceOf[NodeRef[SongDb]])
  }
}

class WrittenBy(graph: OdbGraph, outVertex: NodeRef[ArtistDb], inVertex: NodeRef[SongDb])
  extends OdbEdge(graph, WrittenBy.Label, outVertex, inVertex, WrittenBy.Properties.allAsJava)
