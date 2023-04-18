package overflowdb.traversal.testdomains.gratefuldead

import overflowdb.traversal.Traversal
import overflowdb.{NodeFactory, NodeLayoutInformation, NodeRef, Graph, PropertyKey}

import scala.jdk.CollectionConverters._

class Artist(graph: Graph, id: Long) extends NodeRef[ArtistDb](graph, id) {
  override def label: String = Artist.Label

  def name: String = get.name

  /* Artist <-- sungBy --- Song */
  def sangSongs: Traversal[Song] = get.sangSongs

  /* Artist <-- writtenBy --- Song */
  def wroteSongs: Traversal[Song] = get.wroteSongs
}

object Artist {
  val Label = "artist"

  object Properties {
    val Name = new PropertyKey[String](PropertyNames.Name)
  }

  object PropertyNames {
    val Name = "name"
    val all: Set[String] = Set(Name)
  }

  val factory: NodeFactory[ArtistDb] = new NodeFactory[ArtistDb]() {
    override def forLabel: String = Label
    override def createNode(ref: NodeRef[ArtistDb]) = new ArtistDb(ref)
    override def createNodeRef(graph: Graph, id: Long) = new Artist(graph, id)
  }

  val layoutInformation: NodeLayoutInformation =
    new NodeLayoutInformation(
      Label,
      PropertyNames.all.asJava,
      Nil.asJava,
      List(SungBy.layoutInformation, WrittenBy.layoutInformation).asJava
    )
}
