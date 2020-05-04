package io.shiftleft.overflowdb.traversal.testdomains.hierarchical

import io.shiftleft.overflowdb.traversal.{NodeRefOps, PropertyKey, Traversal}
import io.shiftleft.overflowdb.{NodeFactory, NodeLayoutInformation, NodeRef, OdbGraph}

import scala.jdk.CollectionConverters._

class Car(graph: OdbGraph, id: Long) extends NodeRef[CarDb](graph, id) with NodeRefOps[Car] {
  override def label: String = Car.Label
  def name: String = get.name
  override def toString = s"Car(id=$id)"
}

object Car {
  val Label = "car"
  val LabelId = 7

  object Properties {
    val Name = PropertyKey[String](PropertyNames.Name)
  }

  object PropertyNames {
    val Name = "name"
    val all: Set[String] = Set(Name)
  }

  val factory: NodeFactory[CarDb] = new NodeFactory[CarDb]() {
    override def forLabel: String = Label
    override def forLabelId() = LabelId
    override def createNode(ref: NodeRef[CarDb]) = new CarDb(ref)
    override def createNodeRef(graph: OdbGraph, id: Long) = new Car(graph, id)
  }

  val layoutInformation: NodeLayoutInformation =
    new NodeLayoutInformation(
      LabelId,
      PropertyNames.all.asJava,
      List().asJava,
      List().asJava
    )
}
