package io.shiftleft.overflowdb.traversal.testdomains.hierarchical

import io.shiftleft.overflowdb.traversal.{NodeRefOps, PropertyKey, Traversal}
import io.shiftleft.overflowdb.{NodeFactory, NodeLayoutInformation, NodeRef, OdbGraph}
import scala.jdk.CollectionConverters._

class Elephant(graph: OdbGraph, id: Long) extends NodeRef[ThingDb](graph, id) with NodeRefOps[Elephant] {
  override def label: String = Elephant.Label
  def name: String = get.name
  override def toString = s"Elephant(id=$id)"
}


object Elephant {
  val Label = "thing"
  val LabelId = 6

  object Properties {
    val Name = PropertyKey[String](PropertyNames.Name)
  }

  object PropertyNames {
    val Name = "name"
    val all: Set[String] = Set(Name)
  }

  val factory: NodeFactory[ThingDb] = new NodeFactory[ThingDb]() {
    override def forLabel: String = Label
    override def forLabelId() = LabelId
    override def createNode(ref: NodeRef[ThingDb]) = new ThingDb(ref)
    override def createNodeRef(graph: OdbGraph, id: Long) = new Elephant(graph, id)
  }

  val layoutInformation: NodeLayoutInformation =
    new NodeLayoutInformation(
      LabelId,
      PropertyNames.all.asJava,
      List(Connection.layoutInformation).asJava,
      List(Connection.layoutInformation).asJava
    )
}
