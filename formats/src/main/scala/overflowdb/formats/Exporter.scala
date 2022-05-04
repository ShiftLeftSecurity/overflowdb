package overflowdb.formats

import overflowdb.Graph
import java.nio.file.Path

trait Exporter {
  def runExport(graph: Graph, outputFile: Path): Seq[Path]

  def runExport(graph: Graph, outputFile: String): Seq[Path] =
    runExport(graph, Path.of(outputFile))
}
