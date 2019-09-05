package io.shiftleft.overflowdb.testdomains.gratefuldead

import io.shiftleft.overflowdb._
import scala.jdk.CollectionConverters._

object SungBy {
  val Label = "sungBy"
  object PropertyKeys {
    val all: Set[String] = Set.empty
    val allAsJava: java.util.Set[String] = all.asJava
  }
  val layoutInformation = new EdgeLayoutInformation(Label, PropertyKeys.allAsJava)
  var factory: EdgeFactory[SungBy] = new EdgeFactory[SungBy] {
    override def forLabel(): String = SungBy.Label

    override def createEdge(graph: OdbGraph, outNode: NodeRef[OdbNode], inNode: NodeRef[OdbNode]): SungBy =
      new SungBy(graph, outNode, inNode)
  }
}

class SungBy(graph: OdbGraph, outVertex: NodeRef[_ <: OdbNode], inVertex: NodeRef[_ <: OdbNode])
  extends OdbEdge(graph, SungBy.Label, outVertex, inVertex, SungBy.PropertyKeys.allAsJava)
