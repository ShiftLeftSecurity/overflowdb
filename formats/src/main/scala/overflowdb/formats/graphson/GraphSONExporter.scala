package overflowdb.formats.graphson

import overflowdb.formats._
import overflowdb.{Element, Graph}

import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters.{CollectionHasAsScala, IteratorHasAsScala, MapHasAsScala}
import overflowdb.formats.graphson.GraphSONProtocol._
import spray.json._

import java.util

/**
  * Exports OverflowDB graph to GraphSON 3.0
  *
  * https://tinkerpop.apache.org/docs/3.4.1/dev/io/#graphson-3d0
  */
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
      .map(
        node =>
          Vertex(
            LongValue(node.id),
            node.label,
            propertyEntry(node, propertyId, "g:VertexProperty")
        ))
      .toSeq
    val edgeEntries = graph
      .edges()
      .asScala
      .map { edge =>
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

  def propertyEntry(
      element: Element,
      propertyId: AtomicInteger,
      propertyType: String
  ): Map[String, Property] = {
    element.propertiesMap.asScala.map {
      case (propertyName, propertyValue) =>
        propertyName -> Property(LongValue(propertyId.getAndIncrement()),
                                 valueEntry(propertyValue),
                                 propertyType)
    }.toMap
  }

  def valueEntry(propertyValue: Any): PropertyValue = {
    // Other types require explicit type definitions to be interpreted other than string or bool
    propertyValue match {
      case x: Array[_]     => ListValue(x.map(valueEntry))
      case x: util.List[_] => ListValue(x.asScala.map(valueEntry).toArray)
      case x: Boolean      => BooleanValue(x)
      case x: String       => StringValue(x)
      case x: Double       => DoubleValue(x)
      case x: Float        => FloatValue(x)
      case x: Int          => IntValue(x)
      case x: Long         => LongValue(x)
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
