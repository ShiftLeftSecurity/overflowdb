package overflowdb.formats.graphson

import overflowdb.formats._
import overflowdb.formats.graphson.GraphSONProtocol._
import overflowdb.{Element, Node}
import spray.json._

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters.{IterableHasAsScala, MapHasAsScala}

/** Exports OverflowDB graph to GraphSON 3.0
  *
  * https://tinkerpop.apache.org/docs/3.4.1/dev/io/#graphson-3d0
  */
object GraphSONExporter extends Exporter {

  override def defaultFileExtension = "json"

  override def runExport(
      nodes: IterableOnce[Node],
      edges: IterableOnce[overflowdb.Edge],
      outputFile: Path
  ): ExportResult = {
    val outFile = resolveOutputFileSingle(outputFile, s"export.$defaultFileExtension")
    // OverflowDB only stores IDs on nodes. GraphSON requires IDs on properties and edges too
    // so we add them synthetically
    val propertyId = new AtomicInteger(0)
    val edgeId = new AtomicInteger(0)

    val nodeEntries = nodes.iterator
      .map(node =>
        Vertex(
          LongValue(node.id),
          node.label,
          propertyEntry(node, propertyId, "g:VertexProperty")
        )
      )
      .toSeq

    val edgeEntries = edges.iterator.map { edge =>
      val inNode = edge.inNode()
      val outNode = edge.outNode()
      Edge(
        LongValue(edgeId.getAndIncrement),
        edge.label,
        inNode.label,
        outNode.label,
        LongValue(inNode.id),
        LongValue(outNode.id),
        propertyEntry(edge, propertyId, "g:Property")
      )
    }.toSeq

    val graphSON = GraphSON(GraphSONElements(nodeEntries, edgeEntries))
    val json = graphSON.toJson
    writeFile(outFile, json.prettyPrint)

    ExportResult(
      nodeCount = nodeEntries.size,
      edgeCount = edgeEntries.size,
      files = Seq(outFile),
      Option.empty
    )
  }

  def propertyEntry(
      element: Element,
      propertyId: AtomicInteger,
      propertyType: String
  ): Map[String, Property] = {
    element.propertiesMap.asScala.map { case (propertyName, propertyValue) =>
      propertyName -> Property(LongValue(propertyId.getAndIncrement()), valueEntry(propertyValue), propertyType)
    }.toMap
  }

  def valueEntry(propertyValue: Any): PropertyValue = {
    // Other types require explicit type definitions to be interpreted other than string or bool
    propertyValue match {
      case x: Array[_]              => ListValue(x.map(valueEntry))
      case x: Iterable[_]           => ListValue(x.map(valueEntry).toArray)
      case x: IterableOnce[_]       => ListValue(x.iterator.map(valueEntry).toArray)
      case x: java.lang.Iterable[_] => ListValue(x.asScala.map(valueEntry).toArray)
      case x: Boolean               => BooleanValue(x)
      case x: String                => StringValue(x)
      case x: Double                => DoubleValue(x)
      case x: Float                 => FloatValue(x)
      case x: Int                   => IntValue(x)
      case x: Long                  => LongValue(x)
    }
  }

}
