package overflowdb.traversal.testdomains.hierarchical

import overflowdb.{NodeFactory, NodeLayoutInformation, NodeRef, Graph, PropertyKey}

import scala.jdk.CollectionConverters._

class Car(graph: Graph, id: Long) extends NodeRef[CarDb](graph, id) {
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
    override def createNodeRef(graph: Graph, id: Long) = new Car(graph, id)
  }

  val layoutInformation: NodeLayoutInformation =
    new NodeLayoutInformation(
      LabelId,
      PropertyNames.all.asJava,
      List().asJava,
      List().asJava
    )
}
