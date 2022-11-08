package overflowdb.formats

import java.nio.file.{Path, Paths}
import overflowdb.{Edge, Graph, Node}
import scala.jdk.CollectionConverters.IteratorHasAsScala


trait Exporter {

  def defaultFileExtension: String

  def runExport(nodes: IterableOnce[Node], edges: IterableOnce[Edge], outputFile: Path): ExportResult

  def runExport(graph: Graph, outputFile: Path): ExportResult =
    runExport(graph.nodes().asScala, graph.edges().asScala, outputFile)

  def runExport(graph: Graph, outputFile: String): ExportResult =
    runExport(graph, Paths.get(outputFile))
}

case class ExportResult(nodeCount: Int, edgeCount: Int, files: Seq[Path], additionalInfo: Option[String])