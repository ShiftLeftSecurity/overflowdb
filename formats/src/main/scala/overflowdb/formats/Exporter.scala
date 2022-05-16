package overflowdb.formats

import overflowdb.Graph
import java.nio.file.Path

trait Exporter {
  def runExport(graph: Graph, outputFile: Path): ExportResult

  def runExport(graph: Graph, outputFile: String): ExportResult =
    runExport(graph, Path.of(outputFile))
}

case class ExportResult(nodeCount: Int, edgeCount: Int, files: Seq[Path], additionalInfo: String)