package overflowdb.formats.neo4jcsv

import overflowdb.Graph
import overflowdb.formats.Exporter

import java.nio.file.Path

object Neo4jCsvExporter extends Exporter {

  override def runExport(graph: Graph, outputRootDirectory: Path): Seq[Path] = {
    ???
  }

}
