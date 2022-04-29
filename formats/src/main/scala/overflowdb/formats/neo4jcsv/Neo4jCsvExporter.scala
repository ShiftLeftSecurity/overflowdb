package overflowdb.formats.neo4jcsv

import com.github.tototoshi.csv.CSVWriter
import overflowdb.Graph
import overflowdb.formats.Exporter
import overflowdb.traversal.Traversal

import java.io.File
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
    val dataFile   = outputRootDirectory.resolve(s"$label.csv").toFile
    val headerFile = outputRootDirectory.resolve(s"$label$HeaderFileSuffix.csv").toFile

    val propertyNamesOrdered = graph.nodes(label).next.propertyKeys().asScala.toSeq.sorted
    val columnDefByName = new ColumnDefByName

    Using(CSVWriter.open(dataFile, append = false)) { writer =>
      Traversal(graph.nodes(label)).foreach { node =>
        val rowBuilder = Seq.newBuilder[String]

        // first the 'special' columns ID and LABEL
        rowBuilder.addOne(node.id.toString)
        rowBuilder.addOne(node.label)

        propertyNamesOrdered.foreach { propertyName =>
          val entry = node.propertyOption(propertyName).toScala match {
            case None => ""
            case Some(value) =>
              columnDefByName.updateWith(propertyName, value) match {
                case ScalarColumnDef(_) => value.toString
                case ArrayColumnDef(_, iteratorAccessor) =>
                  /**
                   * Note: if all instances of this array property type are empty, we will not have
                   * the valueType (because it's derived from the runtime class). At the same time, it doesn't matter
                   * for serialization, because the csv entry is always empty for all empty arrays.
                   */
                  iteratorAccessor(value).mkString(";")
              }
          }
          rowBuilder.addOne(entry)
        }
        writer.writeRow(rowBuilder.result)
      }
    }.get

    Using(CSVWriter.open(headerFile, append = false)) { writer =>
      val propertiesWithTypes = propertyNamesOrdered.map { name =>
        columnDefByName.get(name) match {
          case Some(ScalarColumnDef(valueType)) =>
            s"$name:$valueType"
          case Some(ArrayColumnDef(Some(valueType), _)) =>
            s"$name:$valueType[]"
          case _ =>
            name
        }
      }
      writer.writeRow(
        Seq(ColumnType.Id, ColumnType.Label) ++ propertiesWithTypes
      )
    }.get

    Seq(headerFile.toPath, dataFile.toPath)
  }

  private def exportEdges(graph: Graph, outputRootDirectory: Path): Seq[Path] = {
    val edgeFilesContextByLabel = mutable.Map.empty[String, EdgeFilesContext]
    graph.edges().forEachRemaining { edge =>
      val writer = edgeFilesContextByLabel.getOrElseUpdate(edge.label(), {
        // first time we encounter an edge of this type - create the columnMapping and write the header file
        // TODO
        ???
      })

      // TODO write row
    }

    edgeFilesContextByLabel.values.flatMap { case EdgeFilesContext(headerFile, dataFile, writer) =>
      writer.flush()
      writer.close()
      Seq(headerFile, dataFile).map(_.toPath)
    }.toSeq
  }


  private case class EdgeFilesContext(headerFile: File, dataFile: File, writer: CSVWriter)
}