package overflowdb.formats.neo4jcsv

import com.github.tototoshi.csv.CSVWriter
import overflowdb.Graph
import overflowdb.formats.Exporter
import overflowdb.traversal.Traversal

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
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
      val dataFile   = outputRootDirectory.resolve(s"$label.csv").toFile
      val headerFile = outputRootDirectory.resolve(s"$label$HeaderFileSuffix.csv").toFile

      /** we first write the data file and collect all property names and their types (derived from their runtime types)
       *  which are in use */
      var nextColumnIndex = new AtomicInteger(2) // reserving 0 for ID and 1 for LABEL
//      val usedPropertyNames = mutable.Set.empty[String]
      val propertyIndexByName = mutable.Map.empty[String, Int]
      val propertyTypeByName = mutable.Map.empty[String, ColumnType.Value]
//      graph.nodes(label).forEachRemaining { node =>
//        node.propertyKeys().forEach(propertyNames.add)
//      }
//      val propertyNamesOrdered = propertyNames.toSeq.sorted

      Using(CSVWriter.open(dataFile, append = false)) { writer =>
        Traversal(graph.nodes(label)).foreach { node =>
          val rowBuilder = Seq.newBuilder[String]

          // first the 'special' columns ID and LABEL
          rowBuilder.addOne(node.id.toString)
          rowBuilder.addOne(node.label)

          node.propertiesMap().forEach { (name, value) =>
            val propertyIndex = propertyIndexByName.getOrElseUpdate(name, nextColumnIndex.getAndIncrement())

            // note: this ignores the edge case that there may be different runtime types for the same property
            val tpe = propertyTypeByName.getOrElseUpdate(name, deriveNeo4jType(value.getClass))

            
          }

//          propertyNamesOrdered.foreach { propertyName =>
//            rowBuilder.addOne(node.propertyOption(propertyName).toScala.map(_.toString).getOrElse(""))
//          }

          writer.writeRow(rowBuilder.result)
        }
      }

      Using(CSVWriter.open(headerFile, append = false)) { writer =>
        writer.writeRow(
          Seq(ColumnType.Id, ColumnType.Label) ++ propertyNamesOrdered
        )
      }

      Seq(headerFile.toPath, dataFile.toPath)
    }
  }

  private def deriveNeo4jType(value: Class[_]): ColumnType.Value = {
    ???
  }

}