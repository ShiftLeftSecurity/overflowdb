package overflowdb.formats

package object neo4jcsv {

  private[neo4jcsv] object FileType extends Enumeration {
    val Nodes = Value
    val Relationships = Value
  }

  val HeaderFileSuffix = "_header"
  val DataFileSuffix   = "_data"

  object ColumnType extends Enumeration {
    // defining 'stable' string so we can pattern match on them
    val LabelMarker = ":LABEL"
    val TypeMarker = ":TYPE"
    val ArrayMarker = "[]"

    // special types for nodes
    val Id = Value(":ID")
    val Label = Value(LabelMarker)

    // special types for relationships
    val Type = Value(TypeMarker)
    val StartId = Value(":START_ID")
    val EndId = Value(":END_ID")

    // regular data types
    val Int = Value("int")
    val Long = Value("long")
    val Float = Value("float")
    val Double = Value("double")
    val Boolean = Value("boolean")
    val Byte = Value("byte")
    val Short = Value("short")
    val Char = Value("char")
    val String = Value("string")
    val Point = Value("point")
    val Date = Value("date")
    val LocalTime = Value("localtime")
    val Time = Value("time")
    val LocalDateTime = Value("localdatetime")
    val DateTime = Value("datetime")
    val Duration = Value("duration")
  }


}
