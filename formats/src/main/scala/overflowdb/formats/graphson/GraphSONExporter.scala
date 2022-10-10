package overflowdb.formats.graphson

import overflowdb.formats._
import overflowdb.{Element, Graph}

import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters.{IteratorHasAsScala, MapHasAsScala}
import overflowdb.formats.graphson.GraphSONProtocol._
import spray.json._

object GraphSONExporter extends Exporter {

  override def runExport(graph: Graph, outputRootDirectory: Path): ExportResult = {
    val outFile = resolveOutputFile(outputRootDirectory)
    // OverflowDB only stores IDs on nodes. GraphSON requires IDs on properties and edges too
    // so we add them synthetically
    val propertyId = new AtomicInteger(0)
    val edgeId = new AtomicInteger(0)

    val nodeEntries = graph
      .nodes()
      .asScala
      .map(node => Vertex(LongValue(node.id), node.label, vertexProperties(node, propertyId)))
      .toSeq
    val edgeEntries = graph
      .edges()
      .asScala
      .map { edge =>
        val inNode = edge.inNode()
        val outNode = edge.outNode()
        Edge(LongValue(edgeId.getAndIncrement),
             edge.label,
             inNode.label,
             outNode.label,
             LongValue(inNode.id),
             LongValue(outNode.id),
             edgeProperties(edge, propertyId))
      }
      .toSeq
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

  def vertexProperties(
      element: Element,
      propertyId: AtomicInteger
  ): Map[String, VertexProperty] = {
    element.propertiesMap.asScala.map {
      case (propertyName, propertyValue) =>
        propertyName -> VertexProperty(LongValue(propertyId.getAndIncrement()),
                                       valueEntry(propertyValue))
    }.toMap
  }

  def edgeProperties(
      element: Element,
      propertyId: AtomicInteger
  ): Map[String, Property] = {
    element.propertiesMap.asScala.map {
      case (propertyName, propertyValue) =>
        propertyName -> Property(LongValue(propertyId.getAndIncrement()), valueEntry(propertyValue))
    }.toMap
  }

  def valueEntry(propertyValue: Any): PropertyValue = {
    // Other types require explicit type definitions to be interpreted other than string or bool
    propertyValue match {
      case x: Seq[_]  => ListValue(x.map(valueEntry))
      case x: Boolean => BooleanValue(x)
      case x: String  => StringValue(x)
      case x: Double  => DoubleValue(x)
      case x: Float   => FloatValue(x)
      case x: Int     => IntValue(x)
      case x: Long    => LongValue(x)
    }
  }

  private def resolveOutputFile(outputRootDirectory: Path): Path = {
    if (Files.exists(outputRootDirectory)) {
      assert(Files.isDirectory(outputRootDirectory),
             s"given output directory `$outputRootDirectory` must be a directory, but isn't...")
    } else {
      Files.createDirectories(outputRootDirectory)
    }
    outputRootDirectory.resolve("export.graphson")
  }

}
