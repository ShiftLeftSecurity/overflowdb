package io.shiftleft.overflowdb.traversal.testdomains.hierarchical

import io.shiftleft.overflowdb.traversal.{NodeRefOps, PropertyKey, Traversal}
import io.shiftleft.overflowdb.{NodeFactory, NodeLayoutInformation, NodeRef, OdbGraph}

import scala.jdk.CollectionConverters._

class Elephant(graph: OdbGraph, id: Long) extends NodeRef[ElephantDb](graph, id) with NodeRefOps[Elephant] with Mammal {
  override def label: String = Elephant.Label
  override def species = "Elephant"
  override def canSwim = true
  override def toString = s"Elephant(id=$id)"
  def name: String = get.name
}

object Elephant {
  val Label = "elephant"
  val LabelId = 7

  object Properties {
    val Name = PropertyKey[String](PropertyNames.Name)
  }

  object PropertyNames {
    val Name = "name"
    val all: Set[String] = Set(Name)
  }

  val factory: NodeFactory[ElephantDb] = new NodeFactory[ElephantDb]() {
    override def forLabel: String = Label
    override def forLabelId() = LabelId
    override def createNode(ref: NodeRef[ElephantDb]) = new ElephantDb(ref)
    override def createNodeRef(graph: OdbGraph, id: Long) = new Elephant(graph, id)
  }

  val layoutInformation: NodeLayoutInformation =
    new NodeLayoutInformation(
      LabelId,
      PropertyNames.all.asJava,
      List().asJava,
      List().asJava
    )
}
