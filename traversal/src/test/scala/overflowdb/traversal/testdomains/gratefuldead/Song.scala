package overflowdb.traversal.testdomains.gratefuldead

import overflowdb.traversal.Traversal
import overflowdb.{NodeFactory, NodeLayoutInformation, NodeRef, OdbGraph, PropertyKey}

import scala.jdk.CollectionConverters._

class Song(graph: OdbGraph, id: Long) extends NodeRef[SongDb](graph, id) {
  override def label: String = Song.Label

  def name: String = get.name
  def songType: String = get.songType
  def performances: Int = get.performances

  /* Song --- followedBy --- Song */
  def followedBy: Traversal[Song] = get.followedBy

  /* Artist <-- sungBy --- Song */
  def sungBy: Traversal[Artist] = get.sungBy

  /* Artist <-- writtenBy --- Song */
  def writtenBy: Traversal[Artist] = get.writtenBy
}

object Song {
  val Label = "song"
  val LabelId = 5

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
    override def forLabel: String = Label
    override def forLabelId() = LabelId
    override def createNode(ref: NodeRef[SongDb]) = new SongDb(ref)
    override def createNodeRef(graph: OdbGraph, id: Long) = new Song(graph, id)
  }

  val layoutInformation: NodeLayoutInformation =
    new NodeLayoutInformation(
      LabelId,
      PropertyNames.all.asJava,
      List(SungBy.layoutInformation, WrittenBy.layoutInformation, FollowedBy.layoutInformation).asJava,
      List(FollowedBy.layoutInformation).asJava
    )
}


