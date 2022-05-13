package overflowdb.formats.neo4jcsv

import scala.collection.immutable.ArraySeq
import scala.collection.mutable
import scala.jdk.CollectionConverters.IterableHasAsScala

sealed trait ColumnDef
case class ScalarColumnDef(valueType: ColumnType.Value) extends ColumnDef
case class ArrayColumnDef(valueType: Option[ColumnType.Value], iteratorAccessor: Any => Iterable[_]) extends ColumnDef

class ColumnDefinitions(propertyNames: Iterable[String]) {
  private val propertyNamesOrdered = propertyNames.toSeq.sorted
  private val _columnDefByPropertyName = mutable.Map.empty[String, ColumnDef]

  def columnDefByPropertyName(name: String): Option[ColumnDef] = _columnDefByPropertyName.get(name)

  def updateWith(propertyName: String, value: Any): ColumnDef = {
    _columnDefByPropertyName.updateWith(propertyName) {
      case None =>
        // we didn't see this property before - try to derive it's type from the runtime class
        Option(deriveNeo4jType(value))
      case Some(ArrayColumnDef(None, _)) =>
        // value is an array that we've seen before, but we don't have the valueType yet, most likely because previous occurrences were empty arrays
        Option(deriveNeo4jType(value))
      case completeDef =>
        completeDef // we already have everything we need, no need to change anything
    }.get
  }

  /** for header file */
  def propertiesWithTypes: Seq[String] = {
    propertyNamesOrdered.map { name =>
      columnDefByPropertyName(name) match {
        case Some(ScalarColumnDef(valueType)) =>
          s"$name:$valueType"
        case Some(ArrayColumnDef(Some(valueType), _)) =>
          s"$name:$valueType[]"
        case _ =>
          name
      }
    }
  }

  /** for data file
   * updates our internal `_columnDefByPropertyName` model with type information based on runtime values, so that
   * we later have all metadata required for the header file */
  def propertyValues(byNameAccessor: String => Option[_]): Seq[String] = {
    propertyNamesOrdered.map { propertyName =>
      byNameAccessor(propertyName) match {
        case None =>
          "" // property value empty for this element
        case Some(value) =>
          updateWith(propertyName, value) match {
            case ScalarColumnDef(_) =>
              value.toString // scalar property value
            case ArrayColumnDef(_, iteratorAccessor) =>
              /**
               * Array property value - separated by `;` according to the spec
               *
               * Note: if all instances of this array property type are empty, we will not have
               * the valueType (because it's derived from the runtime class). At the same time, it doesn't matter
               * for serialization, because the csv entry is always empty for all empty arrays.
               */
              iteratorAccessor(value).mkString(";")
          }
      }
    }
  }

  /** for cypher file
   * <rant> why does neo4j have 4 different ways to import a CSV, out of which only one works, and really the only
   * help we get is a csv file reader, and we need to specify exactly how each column needs to be parsed and mapped...?
   * </rant>
   */
  def propertiesMappingsForCypher(startIndex: Int): Seq[String] = {
    var idx = startIndex - 1
    propertyNamesOrdered.map { name =>
      idx += 1
      columnDefByPropertyName(name) match {
        case Some(ScalarColumnDef(valueType)) =>
          s"$name: line[$idx]"
        case Some(ArrayColumnDef(Some(valueType), _)) =>
          s"$name: split(line[$idx], \";\")"
        case _ =>
//          name
        ???
      }
    }
  }

  /**
   * maybe choose one of https://neo4j.com/docs/cypher-manual/current/functions/scalar/, depending on the columnType
   */
  private def cypherConversionFunctionMaybe(columnType: ColumnType.Value): Option[String] = {
    columnType match {
      case ColumnType.Id | ColumnType.Int => Some("toInteger")
      case ColumnType.Long => ???
      case ColumnType.Float => ???
      case ColumnType.Double => ???
      case ColumnType.Boolean => ???
      case ColumnType.Byte => ???
      case ColumnType.Short => ???
      case _ => None
    }
  }

  /**
   * derive property types based on the runtime class
   * note: this ignores the edge case that there may be different runtime types for the same property
   * */
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
}
