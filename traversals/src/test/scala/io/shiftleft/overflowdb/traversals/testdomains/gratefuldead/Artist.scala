package io.shiftleft.overflowdb.traversals.testdomains.gratefuldead

import io.shiftleft.overflowdb.traversals.Traversal
import io.shiftleft.overflowdb.{NodeFactory, NodeLayoutInformation, NodeRef, OdbGraph}
import org.apache.tinkerpop.gremlin.structure.Direction

import scala.jdk.CollectionConverters._

object Artist {
  val Label = "artist"

  object Properties {
    val Name = "name"
    val all: Set[String] = Set(Name)
    val allAsJava: java.util.Set[String] = all.asJava
  }

  val factory: NodeFactory[ArtistDb] = new NodeFactory[ArtistDb]() {
    override def forLabel: String = Artist.Label
    override def createNode(ref: NodeRef[ArtistDb]) = new ArtistDb(ref)
    override def createNodeRef(graph: OdbGraph, id: Long) = new Artist(graph, id)
  }

  val layoutInformation: NodeLayoutInformation =
    new NodeLayoutInformation(
      Properties.allAsJava,
      Nil.asJava,
      List(SungBy.layoutInformation, WrittenBy.layoutInformation).asJava
    )
}

class Artist(graph: OdbGraph, id: Long) extends NodeRef[ArtistDb](graph, id) {
  override def label: String = Artist.Label

  def name: String = get.name

  /* Artist <-- sungBy --- Song */
  def sangSongs: Traversal[Song] =
    new Traversal(vertices(Direction.IN, SungBy.Label).asScala.map(_.asInstanceOf[Song]))
}

