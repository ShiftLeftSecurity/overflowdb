package overflowdb.formats.neo4jcsv

import com.github.tototoshi.csv.CSVWriter
import overflowdb.Graph
import overflowdb.formats.Exporter
import overflowdb.traversal.Traversal

import java.nio.file.Path
import scala.collection.immutable.ArraySeq
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
                columnDefByName.updateWith(propertyName) {
                  case None =>
                    // we didn't see this property before - try to derive it's type from the runtime class
                    Option(deriveNeo4jType(value))
                  case Some(ArrayColumnDef(None, _)) =>
                    // value is an array that we've seen before, but we don't have the valueType yet, most likely because previous occurrences were empty arrays
                    Option(deriveNeo4jType(value))
                  case completeDef =>
                    completeDef // we already have the valueType, no need to change anything
                }.get match {
                  case ScalarColumnDef(_) => value.toString
                  case ArrayColumnDef(_, iteratorAccessor) =>
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
  }

  /**
   * derive property types based on the runtime class
   * note: this ignores the edge case that there may be different runtime types for the same property
   *  */
  private def deriveNeo4jType(value: Any): ColumnDef = {
    def deriveNeo4jTypeForArray(iteratorAccessor: Any => Iterable[_]): ArrayColumnDef = {
      // Iterable is immutable, so we can safely (try to) get it's first element
      val valueTypeMaybe = iteratorAccessor(value)
        .iterator
        .nextOption()
        .map(_.getClass)
        .map(deriveNeo4jTypeForScalarValue)
      ArrayColumnDef(valueTypeMaybe, iteratorAccessor)
    }

    value match {
      case _: Iterable[_] =>
        deriveNeo4jTypeForArray(_.asInstanceOf[Iterable[_]])
      case _: IterableOnce[_] =>
        deriveNeo4jTypeForArray(_.asInstanceOf[IterableOnce[_]].iterator.toSeq)
      case _: java.lang.Iterable[_] =>
        deriveNeo4jTypeForArray(_.asInstanceOf[java.lang.Iterable[_]].asScala)
      case _: Array[_] =>
        deriveNeo4jTypeForArray(x => ArraySeq.unsafeWrapArray(x.asInstanceOf[Array[_]]))
      case scalarValue =>
        ScalarColumnDef(deriveNeo4jTypeForScalarValue(scalarValue.getClass))
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

  private sealed trait ColumnDef
  private case class ScalarColumnDef(valueType: ColumnType.Value) extends ColumnDef
  private case class ArrayColumnDef(valueType: Option[ColumnType.Value], iteratorAccessor: Any => Iterable[_]) extends ColumnDef
}