package overflowdb.formats.neo4jcsv

import overflowdb.Graph
import overflowdb.formats.Exporter

import java.nio.file.Path
import scala.jdk.CollectionConverters.MapHasAsScala

object Neo4jCsvExporter extends Exporter {

  override def runExport(graph: Graph, outputRootDirectory: Path): Seq[Path] = {
    val labelsWithNodes = graph.nodeCountByLabel.asScala.collect {
      case (label, count) if count > 0 => label
    }.toSeq

    labelsWithNodes.flatMap { label =>
      // TODO use traversal
//      graph.nodes(label)
      val headerFile = ???
      val dataFile = ???

      Seq(headerFile, dataFile)
    }
  }

}
