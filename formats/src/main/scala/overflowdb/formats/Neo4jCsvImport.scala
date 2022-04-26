package overflowdb.formats

import com.github.tototoshi.csv._
import overflowdb.Graph

import java.nio.file.Path
import scala.util.Using

/**
 * Imports from neo4j csv files
 * see https://neo4j.com/docs/operations-manual/current/tools/neo4j-admin/neo4j-admin-import/
 * */
object Neo4jCsvImport extends Importer {

  override def runImport(graph: Graph, inputFiles: Seq[Path]): Unit = {
    var importedNodeCount = 0
    groupInputFiles(inputFiles).foreach { case HeaderAndDataFile(headerFile, dataFile) =>
      val columnDefs = parseHeaderFile(headerFile)

      Using(CSVReader.open(dataFile.toFile)) { dataReader =>
        dataReader.iterator.zipWithIndex.foreach { case (columns, idx) =>
          assert(columns.size == columnDefs.size, s"datafile row must have the same column count as the headerfile (${columnDefs.size}) - instead found ${columns.size} for row=${columns.mkString(",")}")
          parseRowData(columns, lineNo = idx + 1, columnDefs) match {
            case ParsedRowData(id, label, properties) =>
              val propertiesAsKeyValues = properties.flatMap(parsedProperty => Seq(parsedProperty.name, parsedProperty.value))
              graph.addNode(id, label, propertiesAsKeyValues: _*)
              importedNodeCount += 1
          }
        }
      }.get
    }
    logger.info(s"imported $importedNodeCount nodes")
  }

  private def groupInputFiles(inputFiles: Seq[Path]): Seq[HeaderAndDataFile] = {
    val nonCsvFiles = inputFiles.filterNot(_.getFileName.toString.endsWith(".csv"))
    assert(nonCsvFiles.isEmpty,
      s"input files must all have `.csv` extension, which is not the case for ${nonCsvFiles.mkString(",")}")

    val (headerFiles, dataFiles) = inputFiles.partition(_.getFileName.toString.endsWith("_header.csv"))
    assert(headerFiles.size == dataFiles.size,
      s"number of header files (`xyz_header.csv`) must equal the number of data files (`xyz.csv`)")

    headerFiles.map { headerFile =>
      val wantedBodyFilename = headerFile.getFileName.toString.replaceAll("_header", "")
      dataFiles.find(_.getFileName.toString == wantedBodyFilename) match {
        case Some(dataFile) => HeaderAndDataFile(headerFile, dataFile)
        case None =>
          val inputFilenames = inputFiles.map(_.getFileName).mkString(", ")
          throw new AssertionError(s"body filename `$wantedBodyFilename` wanted, but not found in given input files: $inputFilenames")
      }
    }
  }

  private def parseHeaderFile(headerFile: Path): Map[Int, PropertyDef] = {
    val columnDefs = Using(CSVReader.open(headerFile.toFile)) { headerReader =>
      headerReader.all().headOption.getOrElse(
        throw new AssertionError(s"header file $headerFile is empty"))
    }.get

    val propertyDefs = Map.newBuilder[Int, PropertyDef]
    columnDefs.zipWithIndex.foreach { case (entry, idx) =>
      val propertyDef = entry match {
        case ":LABEL" =>
          PropertyDef("label", Neo4jValueType.Label)
        case s if s.endsWith(":ID") =>
          PropertyDef("id", Neo4jValueType.Id)
        case propertyDef if propertyDef.contains(":") =>
          val name :: valueTpe0 :: Nil = propertyDef.split(':').toList
          val isArray = propertyDef.endsWith("[]") // from the docs: "To define an array type, append [] to the type"
          val valueTpe =
            if (isArray) valueTpe0.dropRight(2)
            else valueTpe0
          PropertyDef(name, valueType = Neo4jValueType.withName(valueTpe), isArray)
        case propertyName =>
          PropertyDef(propertyName, valueType = Neo4jValueType.String)
      }
      propertyDefs.addOne((idx, propertyDef))
    }

    val result = propertyDefs.result()
    List(Neo4jValueType.Id, Neo4jValueType.Label).foreach { valueType =>
      assert(result.find { case (_, propertyDef) => propertyDef.valueType == valueType }.isDefined,
        s"no $valueType column found in headerFile $headerFile - see format definition in https://neo4j.com/docs/operations-manual/current/tools/neo4j-admin/neo4j-admin-import/#import-tool-header-format")
    }
    result
  }

  private def parseRowData(columns: Seq[String], lineNo: Int, columnDefs: Map[Int, PropertyDef]): ParsedRowData = {
    var id: Integer = null
    var label: String = null
    val properties = Seq.newBuilder[ParsedProperty]
    columns.zipWithIndex.foreach { case (entry, idx) =>
      assert(columnDefs.contains(idx), s"column with index=$idx not found in column definitions derived from headerFile")
      columnDefs(idx) match {
        case PropertyDef(_, Neo4jValueType.Id, _) =>
          id = entry.toInt
        case PropertyDef(_, Neo4jValueType.Label, _) =>
          label = entry
        case PropertyDef(name, valueType, false) =>
          if (entry != "" || valueType == Neo4jValueType.String) {
            val value = parsePropertyValue(entry, valueType)
            properties.addOne(ParsedProperty(name, value))
          }
        case PropertyDef(name, valueType, true) =>
          val values = entry.split(';') // from the docs: "By default, array values are separated by ;"
          if (values.nonEmpty && values.head != "") { // csv parser always adds one empty string entry...
            val parsedValues = values.map(parsePropertyValue(_, valueType))
            properties.addOne(ParsedProperty(name, parsedValues))
          }
      }
    }
    assert(id != null, s"no ID column found in row $lineNo")
    assert(label != null, s"no LABEL column found in row $lineNo")

    val ret = ParsedRowData(id, label, properties.result())
    logger.debug("parsed line {}: {}", lineNo, ret)
    ret
  }

  private def parsePropertyValue(rawString: String, valueType: Neo4jValueType.Value): Any = {
    valueType match {
      case Neo4jValueType.Int => rawString.toInt
      case Neo4jValueType.Long => rawString.toLong
      case Neo4jValueType.Float => rawString.toFloat
      case Neo4jValueType.Double => rawString.toDouble
      case Neo4jValueType.Boolean => rawString.toBoolean
      case Neo4jValueType.Byte => rawString.toByte
      case Neo4jValueType.Short => rawString.toShort
      case Neo4jValueType.Char => rawString.head
      case Neo4jValueType.String => rawString
      case Neo4jValueType.Point => ???
      case Neo4jValueType.Date => ???
      case Neo4jValueType.LocalTime => ???
      case Neo4jValueType.Time => ???
      case Neo4jValueType.LocalDateTime => ???
      case Neo4jValueType.DateTime => ???
      case Neo4jValueType.Duration => ???
    }
  }

  private case class HeaderAndDataFile(headerFile: Path, dataFile: Path)
  private case class PropertyDef(name: String, valueType: Neo4jValueType.Value, isArray: Boolean = false)
  private case class ParsedProperty(name: String, value: Any)
  private case class ParsedRowData(id: Int, label: String, properties: Seq[ParsedProperty])

  object Neo4jValueType extends Enumeration {
    type Neo4jValueType = Value
    // special types Id and Label
    val Id = Value(":id")
    val Label = Value(":label")

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