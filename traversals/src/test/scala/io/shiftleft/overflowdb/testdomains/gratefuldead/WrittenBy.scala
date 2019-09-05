package io.shiftleft.overflowdb.testdomains.gratefuldead

import io.shiftleft.overflowdb._
import scala.jdk.CollectionConverters._

object WrittenBy {
  val Label = "writtenBy"
  object PropertyKeys {
    val all: Set[String] = Set.empty
    val allAsJava: java.util.Set[String] = all.asJava
  }
  val layoutInformation = new EdgeLayoutInformation(Label, PropertyKeys.allAsJava)
  var factory: EdgeFactory[WrittenBy] = new EdgeFactory[WrittenBy] {
    override def forLabel(): String = WrittenBy.Label

    override def createEdge(graph: OdbGraph, outNode: NodeRef[OdbNode], inNode: NodeRef[OdbNode]): WrittenBy =
      new WrittenBy(graph, outNode, inNode)
  }
}

class WrittenBy(graph: OdbGraph, outVertex: NodeRef[_ <: OdbNode], inVertex: NodeRef[_ <: OdbNode])
  extends OdbEdge(graph, WrittenBy.Label, outVertex, inVertex, WrittenBy.PropertyKeys.allAsJava)
