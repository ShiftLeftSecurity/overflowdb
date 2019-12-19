package io.shiftleft.overflowdb.traversal.testdomains.simple

import io.shiftleft.overflowdb.traversal.{NodeRefOps, PropertyKey, Traversal}
import io.shiftleft.overflowdb.{NodeFactory, NodeLayoutInformation, NodeRef, OdbGraph}
import scala.jdk.CollectionConverters._

class Thing(graph: OdbGraph, id: Long) extends NodeRef[ThingDb](graph, id) with NodeRefOps[Thing] {
  override def label: String = Thing.Label

  def name: String = get.name

  /* Thing --- followedBy --- Thing */
  def followedBy: Traversal[Thing] = get.followedBy

  override def toString = s"Thing(id=$id)"
}


object Thing {
  val Label = "thing"

  object Properties {
    val Name = PropertyKey[String](PropertyNames.Name)
  }

  object PropertyNames {
    val Name = "name"
    val all: Set[String] = Set(Name)
  }

  val factory: NodeFactory[ThingDb] = new NodeFactory[ThingDb]() {
    override def forLabel: String = Thing.Label
    override def createNode(ref: NodeRef[ThingDb]) = new ThingDb(ref)
    override def createNodeRef(graph: OdbGraph, id: Long) = new Thing(graph, id)
  }

  val layoutInformation: NodeLayoutInformation =
    new NodeLayoutInformation(
      PropertyNames.all.asJava,
      List(Connection.layoutInformation).asJava,
      List(Connection.layoutInformation).asJava
    )
}
