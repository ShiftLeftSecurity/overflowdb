package io.shiftleft.overflowdb.traversal.testdomains.gratefuldead

import io.shiftleft.overflowdb.traversal.{NodeRefOps, PropertyKey, Traversal}
import io.shiftleft.overflowdb.{NodeFactory, NodeLayoutInformation, NodeRef, OdbGraph}

import scala.jdk.CollectionConverters._

class Artist(graph: OdbGraph, id: Long) extends NodeRef[ArtistDb](graph, id) with NodeRefOps[Artist] {
  override def label: String = Artist.Label

  def name: String = get.name

  /* Artist <-- sungBy --- Song */
  def sangSongs: Traversal[Song] = get.sangSongs
}

object Artist {
  val Label = "artist"
  val LabelId = 4

  object Properties {
    val Name = PropertyKey[String](PropertyNames.Name)
  }

  object PropertyNames {
    val Name = "name"
    val all: Set[String] = Set(Name)
  }

  val factory: NodeFactory[ArtistDb] = new NodeFactory[ArtistDb]() {
    override def forLabel: String = Label
    override def forLabelId() = LabelId
    override def createNode(ref: NodeRef[ArtistDb]) = new ArtistDb(ref)
    override def createNodeRef(graph: OdbGraph, id: Long) = new Artist(graph, id)
  }

  val layoutInformation: NodeLayoutInformation =
    new NodeLayoutInformation(
      LabelId,
      PropertyNames.all.asJava,
      Nil.asJava,
      List(SungBy.layoutInformation, WrittenBy.layoutInformation).asJava
    )
}



