package overflowdb.formats.neo4jcsv

import com.github.tototoshi.csv._
import overflowdb.Graph
import overflowdb.formats.{CountAndFiles, ExportResult, Exporter}

import java.nio.file.{Files, Path}
import scala.collection.mutable
import scala.jdk.CollectionConverters.{CollectionHasAsScala, MapHasAsScala}
import scala.jdk.OptionConverters.RichOptional
import scala.util.Using

object Neo4jCsvExporter extends Exporter {

  /**
   * Exports OverflowDB Graph to neo4j csv files
   * see https://neo4j.com/docs/operations-manual/current/tools/neo4j-admin/neo4j-admin-import/
   *
   * For both nodes and relationships, we first write the data file and to derive the property types from their
   * runtime types. We will write columns for all declared properties, because we only know which ones are
   * actually in use *after* traversing all elements.
   * */
  override def runExport(graph: Graph, outputRootDirectory: Path) = {
    val CountAndFiles(nodeCount, nodeFiles) = labelsWithNodes(graph).map { label =>
      exportNodes(graph, label, outputRootDirectory)
    }.reduce(_.plus(_))
    val CountAndFiles(edgeCount, edgeFiles) = exportEdges(graph, outputRootDirectory)

    ExportResult(
      nodeCount,
      edgeCount,
      files = nodeFiles ++ edgeFiles,
      Option(s"""instructions on how to import the exported files into neo4j:
         |```
         |cp $outputRootDirectory/*$DataFileSuffix.csv <neo4j_root>/import
         |cd <neo4j_root>
         |find $outputRootDirectory -name 'nodes_*_cypher.csv' -exec bin/cypher-shell -u <neo4j_user> -p <password> --file {} \\;
         |find $outputRootDirectory -name 'edges_*_cypher.csv' -exec bin/cypher-shell -u <neo4j_user> -p <password> --file {} \\;
         |```
         |""".stripMargin)
    )
  }

  private def exportNodes(graph: Graph, label: String, outputRootDirectory: Path): CountAndFiles = {
    val dataFile   = outputRootDirectory.resolve(s"nodes_$label$DataFileSuffix.csv")
    val headerFile = outputRootDirectory.resolve(s"nodes_$label$HeaderFileSuffix.csv")  // to be written at the very end, with complete ColumnDefByName
    val cypherFile = outputRootDirectory.resolve(s"nodes_$label$CypherFileSuffix.csv")
    val columnDefinitions = new ColumnDefinitions(graph.nodes(label).next.propertyKeys.asScala)
    var nodeCount = 0

    Using.resource(CSVWriter.open(dataFile.toFile, append = false)) { writer =>
      graph.nodes(label).forEachRemaining { node =>
        val specialColumns = Seq(node.id.toString, node.label)
        val propertyValueColumns = columnDefinitions.propertyValues(node.propertyOption(_).toScala)
        writer.writeRow(specialColumns ++ propertyValueColumns)
        nodeCount += 1
      }
    }

    writeSingleLineCsv(headerFile, Seq(ColumnType.Id, ColumnType.Label) ++ columnDefinitions.propertiesWithTypes)

    // write cypher file for import into neo4j
    // starting with index=2, because 0|1 are taken by 'special' columns Id|Label
    val cypherPropertyMappings = columnDefinitions.propertiesMappingsForCypher(startIndex = 2).mkString(",\n")
    val cypherQuery =
      s"""LOAD CSV FROM 'file:/nodes_${label}_data.csv' AS line
         |CREATE (:$label {
         |id: toInteger(line[0]),
         |$cypherPropertyMappings
         |});
         |""".stripMargin
    Files.writeString(cypherFile, cypherQuery)

    CountAndFiles(nodeCount, Seq(headerFile, dataFile, cypherFile))
  }

  /** write edges of all labels */
  private def exportEdges(graph: Graph, outputRootDirectory: Path): CountAndFiles = {
    val edgeFilesContextByLabel = mutable.Map.empty[String, EdgeFilesContext]
    var count = 0

    graph.edges().forEachRemaining { edge =>
      val label = edge.label
      val context = edgeFilesContextByLabel.getOrElseUpdate(label, {
        // first time we encounter an edge of this type - create the columnMapping and write the header file
        val headerFile = outputRootDirectory.resolve(s"edges_$label$HeaderFileSuffix.csv")  // to be written at the very end, with complete ColumnDefByName
        val dataFile   = outputRootDirectory.resolve(s"edges_$label$DataFileSuffix.csv")
        val cypherFile   = outputRootDirectory.resolve(s"edges_$label$CypherFileSuffix.csv")
        val dataFileWriter = CSVWriter.open(dataFile.toFile, append = false)
        val columnDefinitions = new ColumnDefinitions(edge.propertyKeys.asScala)
        EdgeFilesContext(label, headerFile, dataFile, cypherFile, dataFileWriter, columnDefinitions)
      })

      val specialColumns = Seq(edge.outNode.id.toString, edge.inNode.id.toString, edge.label)
      val propertyValueColumns = context.columnDefinitions.propertyValues(edge.propertyOption(_).toScala)
      context.dataFileWriter.writeRow(specialColumns ++ propertyValueColumns)
      count += 1
    }

    val files = edgeFilesContextByLabel.values.flatMap {
      case EdgeFilesContext(label, headerFile, dataFile, cypherFile, dataFileWriter, columnDefinitions) =>
        writeSingleLineCsv(headerFile,
          Seq(ColumnType.StartId, ColumnType.EndId, ColumnType.Type) ++ columnDefinitions.propertiesWithTypes)

        dataFileWriter.flush()
        dataFileWriter.close()

        // write cypher file for import into neo4j
        // starting with index=3, because 0|1|2 are taken by 'special' columns StartId|EndId|Type
        val cypherPropertyMappings = columnDefinitions.propertiesMappingsForCypher(startIndex = 3).mkString(",\n")
        val cypherQuery =
          s"""LOAD CSV FROM 'file:/edges_${label}_data.csv' AS line
             |MATCH (a), (b)
             |WHERE a.id = toInteger(line[0]) AND b.id = toInteger(line[1])
             |CREATE (a)-[r:$label {$cypherPropertyMappings}]->(b);
             |""".stripMargin
        Files.writeString(cypherFile, cypherQuery)

        Seq(headerFile, dataFile, cypherFile)
    }.toSeq

    CountAndFiles(count, files)
  }

  private def labelsWithNodes(graph: Graph): Seq[String] = {
    graph.nodeCountByLabel.asScala.collect {
      case (label, count) if count > 0 => label
    }.toSeq
  }

  private def writeSingleLineCsv(outputFile: Path, entries: Seq[Any]): Unit = {
    Using.resource(CSVWriter.open(outputFile.toFile, append = false)) { writer =>
      writer.writeRow(entries)
    }
  }

  private case class EdgeFilesContext(label: String,
                                      headerFile: Path,
                                      dataFile: Path,
                                      cypherFile: Path,
                                      dataFileWriter: CSVWriter,
                                      columnDefinitions: ColumnDefinitions)
}
