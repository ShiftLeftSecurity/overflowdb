package overflowdb.formats

import overflowdb.Graph

trait Exporter {
  def run(graph: Graph, outputFile: String): Unit
}
