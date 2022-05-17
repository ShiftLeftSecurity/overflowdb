package overflowdb.formats.graphml

import overflowdb.Graph
import overflowdb.formats.{CountAndFiles, ExportResult, Exporter}
import java.nio.file.Path

object GraphMLExporter  extends Exporter {

  /**
   * Exports OverflowDB Graph to graphml
   * https://en.wikipedia.org/wiki/GraphML
   * http://graphml.graphdrawing.org/primer/graphml-primer.html
   * */
  override def runExport(graph: Graph, outputRootDirectory: Path) = {
    //    val CountAndFiles(nodeCount, nodeFiles) = labelsWithNodes(graph).map { label =>
    //      exportNodes(graph, label, outputRootDirectory)
    //    }.reduce(_.plus(_))
    //    val CountAndFiles(edgeCount, edgeFiles) = exportEdges(graph, outputRootDirectory)
    //
    //    ExportResult(
    //      nodeCount,
    //      edgeCount,
    //      files = nodeFiles ++ edgeFiles,
    //      s"""instructions on how to import the exported files into neo4j:
    //         |```
    //         |cp $outputRootDirectory/*$DataFileSuffix.csv <neo4j_root>/import
    //         |cd <neo4j_root>
    //         |find $outputRootDirectory -name 'nodes_*_cypher.csv' -exec bin/cypher-shell -u <neo4j_user> -p <password> --file {} \\;
    //         |find $outputRootDirectory -name 'edges_*_cypher.csv' -exec bin/cypher-shell -u <neo4j_user> -p <password> --file {} \\;
    //         |```
    //         |""".stripMargin
    //    )
    ???
  }
}
