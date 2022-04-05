package overflowdb.formats

import overflowdb.Graph

trait Exporter {
  def runExport(graph: Graph, outputFile: String): Unit
}
