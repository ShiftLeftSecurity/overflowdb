package overflowdb.formats.neo4jcsv

import scala.collection.immutable.ArraySeq
import scala.collection.mutable
import scala.jdk.CollectionConverters.IterableHasAsScala

sealed trait ColumnDef
case class ScalarColumnDef(valueType: ColumnType.Value) extends ColumnDef
case class ArrayColumnDef(valueType: Option[ColumnType.Value], iteratorAccessor: Any => Iterable[_]) extends ColumnDef

class ColumnDefByName {
  private val underlying = mutable.Map.empty[String, ColumnDef]

  def get(name: String): Option[ColumnDef] = underlying.get(name)

  def updateWith(propertyName: String, value: Any): ColumnDef = {
    underlying.updateWith(propertyName) {
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
