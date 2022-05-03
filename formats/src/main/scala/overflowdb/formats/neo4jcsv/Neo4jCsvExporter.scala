package overflowdb.formats.neo4jcsv

import com.github.tototoshi.csv._
import overflowdb.Graph
import overflowdb.formats.Exporter
import overflowdb.traversal.Traversal

import java.nio.file.Path
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
  override def runExport(graph: Graph, outputRootDirectory: Path): Seq[Path] = {
    val labelsWithNodes = graph.nodeCountByLabel.asScala.collect {
      case (label, count) if count > 0 => label
    }.toSeq

    val nodeFiles = labelsWithNodes.flatMap(exportNodes(graph, _, outputRootDirectory))
    val edgeFiles = exportEdges(graph, outputRootDirectory)
    nodeFiles ++ edgeFiles
  }

  private def exportNodes(graph: Graph, label: String, outputRootDirectory: Path): Seq[Path] = {
    val dataFile   = outputRootDirectory.resolve(s"nodes_$label.csv")
    val headerFile = outputRootDirectory.resolve(s"nodes_$label$HeaderFileSuffix.csv")  // to be written at the very end, with complete ColumnDefByName
    val columnDefinitions = new ColumnDefinitions(graph.nodes(label).next.propertyKeys.asScala)

    Using(CSVWriter.open(dataFile.toFile, append = false)) { writer =>
      Traversal(graph.nodes(label)).foreach { node =>
        val specialColumns = Seq(node.id.toString, node.label)
        val propertyValueColumns = columnDefinitions.propertyValues(node.propertyOption(_).toScala)
        writer.writeRow(specialColumns ++ propertyValueColumns)
      }
    }.get

    writeSingleLineCsv(headerFile, Seq(ColumnType.Id, ColumnType.Label) ++ columnDefinitions.propertiesWithTypes)
    Seq(headerFile, dataFile)
  }

  /** write edges of all labels */
  private def exportEdges(graph: Graph, outputRootDirectory: Path): Seq[Path] = {
    val edgeFilesContextByLabel = mutable.Map.empty[String, EdgeFilesContext]

    graph.edges().forEachRemaining { edge =>
      val label = edge.label
      val context = edgeFilesContextByLabel.getOrElseUpdate(label, {
        // first time we encounter an edge of this type - create the columnMapping and write the header file
        val headerFile = outputRootDirectory.resolve(s"edges_$label$HeaderFileSuffix.csv")  // to be written at the very end, with complete ColumnDefByName
        val dataFile   = outputRootDirectory.resolve(s"edges_$label.csv")
        val dataFileWriter = CSVWriter.open(dataFile.toFile, append = false)
        val columnDefinitions = new ColumnDefinitions(edge.propertyKeys.asScala)
        EdgeFilesContext(headerFile, dataFile, dataFileWriter, columnDefinitions)
      })

      val specialColumns = Seq(edge.outNode.id.toString, edge.inNode.id.toString, edge.label)
      val propertyValueColumns = context.columnDefinitions.propertyValues(edge.propertyOption(_).toScala)
      context.dataFileWriter.writeRow(specialColumns ++ propertyValueColumns)
    }

    edgeFilesContextByLabel.values.flatMap {
      case EdgeFilesContext(headerFile, dataFile, dataFileWriter, columnDefinitions) =>
        writeSingleLineCsv(headerFile,
          Seq(ColumnType.StartId, ColumnType.EndId, ColumnType.Type) ++ columnDefinitions.propertiesWithTypes)

        dataFileWriter.flush()
        dataFileWriter.close()
        Seq(headerFile, dataFile)
    }.toSeq
  }

  private def writeSingleLineCsv(outputFile: Path, entries: Seq[Any]): Unit = {
    Using(CSVWriter.open(outputFile.toFile, append = false)) { writer =>
      writer.writeRow(entries)
    }.get
  }

  private case class EdgeFilesContext(headerFile: Path,
                                      dataFile: Path,
                                      dataFileWriter: CSVWriter,
                                      columnDefinitions: ColumnDefinitions)
}
