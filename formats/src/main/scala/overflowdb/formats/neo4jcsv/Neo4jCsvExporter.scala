package overflowdb.formats.neo4jcsv

import com.github.tototoshi.csv.CSVWriter
import overflowdb.Graph
import overflowdb.formats.Exporter
import overflowdb.traversal.Traversal

import java.nio.file.Path
import scala.collection.mutable
import scala.jdk.CollectionConverters.{CollectionHasAsScala, MapHasAsScala}
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

      /**
       * We first write the data file and to derive the property types from their runtime types.
       * We will write columns for all declared properties, because we only
       * know which ones are actually in use *after* traversing all nodes.
       *  */
      val propertyNamesOrdered = graph.nodes(label).next.propertyKeys().asScala.toSeq.sorted
      val propertyTypeByName = mutable.Map.empty[String, ColumnType.Value]

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
                // update property types as we go based on the runtime types
                // note: this ignores the edge case that there may be different runtime types for the same property
                if (!propertyTypeByName.contains(propertyName))
                  deriveNeo4jType(value.getClass)

                value.toString
            }
            rowBuilder.addOne(entry)
          }

          writer.writeRow(rowBuilder.result)
        }
      }.get

      Using(CSVWriter.open(headerFile, append = false)) { writer =>
        writer.writeRow(
          Seq(ColumnType.Id, ColumnType.Label) ++ propertyNamesOrdered
        )
      }.get

      Seq(headerFile.toPath, dataFile.toPath)
    }
  }

  private def deriveNeo4jType(tpe: Class[_]): ColumnType.Value = {
//    if (tpe.isAssignableFrom(classOf[String]))
//      ColumnType.String
//    else if (tpe.isAssignableFrom(classOf[Integer]))
//      ColumnType.Int
//    else
      throw new NotImplementedError(s"unable to derive a Neo4j type for given runtime type $tpe")
//    val String = classOf[String]
//    tpe match {
//      case String => ColumnType.String
//    }
  }

}