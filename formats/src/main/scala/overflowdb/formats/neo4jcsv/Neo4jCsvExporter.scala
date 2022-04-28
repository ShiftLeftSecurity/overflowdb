package overflowdb.formats.neo4jcsv

import com.github.tototoshi.csv.CSVWriter
import overflowdb.Graph
import overflowdb.formats.Exporter
import overflowdb.traversal.Traversal

import java.nio.file.Path
import scala.collection.mutable
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.jdk.OptionConverters.RichOptional
import scala.util.Using

object Neo4jCsvExporter extends Exporter {

  override def runExport(graph: Graph, outputRootDirectory: Path): Seq[Path] = {
    val labelsWithNodes = graph.nodeCountByLabel.asScala.collect {
      case (label, count) if count > 0 => label
    }.toSeq

    labelsWithNodes.flatMap { label =>
      val propertyNames = mutable.Set.empty[String]
      graph.nodes(label).forEachRemaining(node =>
        node.propertyKeys().forEach(propertyNames.add)
      )
      val propertyNamesOrdered: Seq[String] =
        propertyNames.toSeq.sorted

      val headerFile = outputRootDirectory.resolve(s"${label}_header.csv").toFile
      val dataFile   = outputRootDirectory.resolve(s"${label}.csv").toFile

      Using(CSVWriter.open(headerFile, append = false)) { writer =>
        writer.writeRow(
          Seq(":ID", ":LABEL") ++ propertyNamesOrdered
        )
      }

      Using(CSVWriter.open(dataFile, append = false)) { writer =>
        Traversal(graph.nodes(label)).foreach { node =>
          val rowBuilder = Seq.newBuilder[String]

          // first the 'special' columns ID and LABEL
          rowBuilder.addOne(node.id.toString)
          rowBuilder.addOne(node.label)

          propertyNamesOrdered.foreach { propertyName =>
            rowBuilder.addOne(node.propertyOption(propertyName).toScala.map(_.toString).getOrElse(""))
          }

          writer.writeRow(rowBuilder.result)
        }
      }

      Seq(headerFile.toPath, dataFile.toPath)
    }
  }

}