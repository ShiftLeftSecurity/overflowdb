package io.shiftleft.overflowdb.traversals.testdomains.gratefuldead

import io.shiftleft.overflowdb.{NodeFactory, NodeLayoutInformation, NodeRef, OdbGraph}

import scala.jdk.CollectionConverters._

object Song {
  val Label = "song"

  object Properties {
    val Name = "name"
    val SongType = "songType"
    val Performances = "performances"
    val all: Set[String] = Set(Name, SongType, Performances)
    val allAsJava: java.util.Set[String] = all.asJava
  }

  val factory: NodeFactory[SongDb] = new NodeFactory[SongDb]() {
    override def forLabel: String = Song.Label
    override def createNode(ref: NodeRef[SongDb]) = new SongDb(ref)
    override def createNodeRef(graph: OdbGraph, id: Long) = new Song(graph, id)
  }

  val layoutInformation: NodeLayoutInformation =
    new NodeLayoutInformation(
      Properties.allAsJava,
      List(SungBy.layoutInformation, WrittenBy.layoutInformation, FollowedBy.layoutInformation).asJava,
      List(FollowedBy.layoutInformation).asJava
    )
}

class Song(graph: OdbGraph, id: Long) extends NodeRef[SongDb](graph, id) {
  override def label: String = Song.Label

  def name: String = get.name
  def songType: String = get.songType
  def performances: Int = get.performances
}

