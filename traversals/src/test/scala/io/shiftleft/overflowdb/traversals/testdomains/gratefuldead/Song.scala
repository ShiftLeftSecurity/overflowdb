package io.shiftleft.overflowdb.traversals.testdomains.gratefuldead

import io.shiftleft.overflowdb.traversals.{NodeRefOps, PropertyKey, Traversal}
import io.shiftleft.overflowdb.{NodeFactory, NodeLayoutInformation, NodeRef, OdbGraph}

import scala.jdk.CollectionConverters._

class Song(graph: OdbGraph, id: Long) extends NodeRef[SongDb](graph, id) with NodeRefOps[Song] {
  override def label: String = Song.Label

  def name: String = get.name
  def songType: String = get.songType
  def performances: Int = get.performances

  /* Song --- followedBy --- Song */
  def followedBy: Traversal[Song] = get.followedBy
}

object Song {
  val Label = "song"

  object Properties {
    val Name = PropertyKey[String](PropertyNames.Name)
    val SongType = PropertyKey[String](PropertyNames.SongType)
    val Performances = PropertyKey[Int](PropertyNames.Performances)
  }

  object PropertyNames {
    val Name = "name"
    val SongType = "songType"
    val Performances = "performances"
    val all: Set[String] = Set(Name, SongType, Performances)
  }

  val factory: NodeFactory[SongDb] = new NodeFactory[SongDb]() {
    override def forLabel: String = Song.Label
    override def createNode(ref: NodeRef[SongDb]) = new SongDb(ref)
    override def createNodeRef(graph: OdbGraph, id: Long) = new Song(graph, id)
  }

  val layoutInformation: NodeLayoutInformation =
    new NodeLayoutInformation(
      PropertyNames.all.asJava,
      List(SungBy.layoutInformation, WrittenBy.layoutInformation, FollowedBy.layoutInformation).asJava,
      List(FollowedBy.layoutInformation).asJava
    )
}


