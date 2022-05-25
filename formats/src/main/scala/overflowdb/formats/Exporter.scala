package overflowdb.formats

import overflowdb.Graph

import java.nio.file.{Path, Paths}

trait Exporter {
  def runExport(graph: Graph, outputFile: Path): ExportResult

  def runExport(graph: Graph, outputFile: String): ExportResult =
    runExport(graph, Paths.get(outputFile))
}

case class ExportResult(nodeCount: Int, edgeCount: Int, files: Seq[Path], additionalInfo: Option[String])