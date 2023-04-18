package overflowdb.formats.neo4jcsv

import com.github.tototoshi.csv._
import overflowdb.formats.{ExportResult, Exporter, writeFile}
import overflowdb.{Edge, Node}

import java.nio.file.Path
import scala.collection.mutable
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.OptionConverters.RichOptional
import scala.util.Using

object Neo4jCsvExporter extends Exporter {

  override def defaultFileExtension = "csv"

  /** Exports OverflowDB Graph to neo4j csv files see
    * https://neo4j.com/docs/operations-manual/current/tools/neo4j-admin/neo4j-admin-import/
    *
    * For both nodes and relationships, we first write the data file and to derive the property types from their runtime
    * types. We will write columns for all declared properties, because we only know which ones are actually in use
    * *after* traversing all elements.
    */
  override def runExport(nodes: IterableOnce[Node], edges: IterableOnce[Edge], outputRootDirectory: Path) = {
    val nodesByLabel = nodes.iterator.toSeq.groupBy(_.label).filter(_._2.nonEmpty)
    val CountAndFiles(nodeCount, nodeFiles) = nodesByLabel
      .map { case (label, nodes) =>
        exportNodes(nodes, label, outputRootDirectory)
      }
      .reduce(_.plus(_))

    val CountAndFiles(edgeCount, edgeFiles) = exportEdges(edges, outputRootDirectory)

    val outputRootAbsolute = outputRootDirectory.toAbsolutePath

    ExportResult(
      nodeCount = nodeCount,
      edgeCount = edgeCount,
      files = nodeFiles ++ edgeFiles,
      additionalInfo = Option(s"""Instructions on how to import the exported files into neo4j:
           |Prerequisite: ensure you have neo4j community server running (enterprise and desktop may work too)
           |e.g. download from https://neo4j.com/download-center/#community and start via `bin/neo4j console`
           |
           |Then, in a new terminal:
           |```
           |cd <neo4j_root>
           |
           |# if you have a fresh instance, you must first change the initial password
           |bin/cypher-shell -u neo4j -p neo4j
           |# exit the cypher shell
           |
           |# copy the data files to the `import` directory, where neo4j will find them
           |cp $outputRootAbsolute/*$DataFileSuffix.csv import
           |
           |find $outputRootAbsolute -name 'nodes_*_cypher.csv' -exec bin/cypher-shell -u neo4j -p <password> --file {} \\;
           |find $outputRootAbsolute -name 'edges_*_cypher.csv' -exec bin/cypher-shell -u neo4j -p <password> --file {} \\;
           |```
           |""".stripMargin)
    )
  }

  private def exportNodes(nodes: IterableOnce[Node], label: String, outputRootDirectory: Path): CountAndFiles = {
    val dataFile = outputRootDirectory.resolve(s"nodes_$label$DataFileSuffix.csv")
    val headerFile = outputRootDirectory.resolve(
      s"nodes_$label$HeaderFileSuffix.csv"
    ) // to be written at the very end, with complete ColumnDefByName
    val cypherFile = outputRootDirectory.resolve(s"nodes_$label$CypherFileSuffix.csv")
    // will be initialized with the first node
    var columnDefinitions: ColumnDefinitions = null
    var nodeCount = 0

    Using.resource(CSVWriter.open(dataFile.toFile, append = false)) { writer =>
      nodes.iterator.foreach { node =>
        if (columnDefinitions == null) columnDefinitions = new ColumnDefinitions(node.propertyKeys.asScala)

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
    writeFile(cypherFile, cypherQuery)

    CountAndFiles(nodeCount, Seq(headerFile, dataFile, cypherFile))
  }

  /** write edges of all labels */
  private def exportEdges(edges: IterableOnce[Edge], outputRootDirectory: Path): CountAndFiles = {
    val edgeFilesContextByLabel = mutable.Map.empty[String, EdgeFilesContext]
    var count = 0

    edges.iterator.foreach { edge =>
      val label = edge.label
      val context = edgeFilesContextByLabel.getOrElseUpdate(
        label, {
          // first time we encounter an edge of this type - create the columnMapping and write the header file
          val headerFile = outputRootDirectory.resolve(
            s"edges_$label$HeaderFileSuffix.csv"
          ) // to be written at the very end, with complete ColumnDefByName
          val dataFile = outputRootDirectory.resolve(s"edges_$label$DataFileSuffix.csv")
          val cypherFile = outputRootDirectory.resolve(s"edges_$label$CypherFileSuffix.csv")
          val dataFileWriter = CSVWriter.open(dataFile.toFile, append = false)
          val columnDefinitions = new ColumnDefinitions(edge.propertyKeys.asScala)
          EdgeFilesContext(label, headerFile, dataFile, cypherFile, dataFileWriter, columnDefinitions)
        }
      )

      val specialColumns = Seq(edge.outNode.id.toString, edge.inNode.id.toString, edge.label)
      val propertyValueColumns = context.columnDefinitions.propertyValues(edge.propertyOption(_).toScala)
      context.dataFileWriter.writeRow(specialColumns ++ propertyValueColumns)
      count += 1
    }

    val files = edgeFilesContextByLabel.values.flatMap {
      case EdgeFilesContext(label, headerFile, dataFile, cypherFile, dataFileWriter, columnDefinitions) =>
        writeSingleLineCsv(
          headerFile,
          Seq(ColumnType.StartId, ColumnType.EndId, ColumnType.Type) ++ columnDefinitions.propertiesWithTypes
        )

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
        writeFile(cypherFile, cypherQuery)

        Seq(headerFile, dataFile, cypherFile)
    }.toSeq

    CountAndFiles(count, files)
  }

  private def writeSingleLineCsv(outputFile: Path, entries: Seq[Any]): Unit = {
    Using.resource(CSVWriter.open(outputFile.toFile, append = false)) { writer =>
      writer.writeRow(entries)
    }
  }

  private case class EdgeFilesContext(
      label: String,
      headerFile: Path,
      dataFile: Path,
      cypherFile: Path,
      dataFileWriter: CSVWriter,
      columnDefinitions: ColumnDefinitions
  )

  case class CountAndFiles(count: Int, files: Seq[Path]) {
    def plus(other: CountAndFiles): CountAndFiles =
      CountAndFiles(count + other.count, files ++ other.files)
  }

}
