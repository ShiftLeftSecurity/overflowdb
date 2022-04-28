package overflowdb.formats.neo4jcsv

import com.github.tototoshi.csv.CSVWriter
import overflowdb.Graph
import overflowdb.formats.Exporter
import overflowdb.traversal.Traversal

import java.nio.file.Path
import scala.collection.mutable
import scala.jdk.CollectionConverters.{CollectionHasAsScala, IterableHasAsScala, IteratorHasAsScala, MapHasAsScala}
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
      val columnDefByName = mutable.Map.empty[String, ColumnDef]

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
                val iteratorAccessorXXXXXXXX = columnDefByName.getOrElseUpdate(propertyName, {
                  deriveNeo4jType(value).get
                  /* TODO drop the `Option` around return type here?
                   * context: property may be an array, but because it's empty we cannot yet determine it's value type...
                   * maybe we need somethihng like
                   * ```
                     private sealed trait ColumnDef
                     private case class ScalarColumnDef(valueType: ColumnType.Value)
                     private case class ArrayColumnDef(valueType: Option[ColumnType.Value], iterator: Any => Iterable[_])

                   * ```
                   * and invoke columnDefByName.updated()

                   */
                })
                if (!columnDefByName.contains(propertyName)) {
                  deriveNeo4jType(value).foreach { columnDef =>
                    columnDefByName.update(propertyName, columnDef)
                  }
                }

                // TODO properly write values for arrays etc... using columnDef.iterAccessor

                value.toString
            }
            rowBuilder.addOne(entry)
          }
          writer.writeRow(rowBuilder.result)
        }
      }.get

      Using(CSVWriter.open(headerFile, append = false)) { writer =>
        val propertiesWithTypes = propertyNamesOrdered.map { name =>
          columnDefByName.get(name) match {
            case Some(columnDef) if columnDef.isScalarValue =>
              s"$name:${columnDef.valueType}"
            case Some(columnDef) =>
              s"$name:${columnDef.valueType}[]"
            case None =>
              name
          }
        }
        writer.writeRow(
          Seq(ColumnType.Id, ColumnType.Label) ++ propertiesWithTypes
        )
      }.get

      Seq(headerFile.toPath, dataFile.toPath)
    }
  }

  private def deriveNeo4jType(value: Any): Option[ColumnDef] = {
    value match {
      case iter: Iterable[_] => // Iterable is immutable, so we can safely to get it's first element
        iter.iterator.nextOption().map(value =>
          ColumnDef(
            deriveNeo4jTypeForScalarValue(value.getClass),
            Some(iter)
          )
        )
      case iter: IterableOnce[_] =>
        deriveNeo4jType(iter.iterator.toSeq: Iterable[_])
      case iter: java.lang.Iterable[_] =>
        deriveNeo4jType(iter.asScala: Iterable[_])
      case array: Array[_] =>
      deriveNeo4jType(array: Iterable[_])
      case scalarValue =>
        Option(ColumnDef(deriveNeo4jTypeForScalarValue(scalarValue.getClass), arrayIterator = None))
    }
  }

  private def deriveNeo4jTypeForScalarValue(tpe: Class[_]): ColumnType.Value = {
    if (tpe.isAssignableFrom(classOf[String]))
      ColumnType.String
    else if (tpe.isAssignableFrom(classOf[Int]) || tpe.isAssignableFrom(classOf[Integer]))
      ColumnType.Int
    else if (tpe.isAssignableFrom(classOf[Long]) || tpe.isAssignableFrom(classOf[java.lang.Long]))
      ColumnType.Long
    else if (tpe.isAssignableFrom(classOf[Float]) || tpe.isAssignableFrom(classOf[java.lang.Float]))
      ColumnType.Float
    else if (tpe.isAssignableFrom(classOf[Double]) || tpe.isAssignableFrom(classOf[java.lang.Double]))
      ColumnType.Double
    else if (tpe.isAssignableFrom(classOf[Boolean]) || tpe.isAssignableFrom(classOf[java.lang.Boolean]))
      ColumnType.Boolean
    else if (tpe.isAssignableFrom(classOf[Byte]) || tpe.isAssignableFrom(classOf[java.lang.Byte]))
      ColumnType.Byte
    else if (tpe.isAssignableFrom(classOf[Short]) || tpe.isAssignableFrom(classOf[java.lang.Short]))
      ColumnType.Short
    else if (tpe.isAssignableFrom(classOf[Char]))
      ColumnType.Char
    else
      throw new NotImplementedError(s"unable to derive a Neo4j type for given runtime type $tpe")
  }

  private case class ColumnDef(valueType: ColumnType.Value, arrayIterator: Option[Iterable[_]]) {
    def isScalarValue = arrayIterator.isEmpty
  }
}