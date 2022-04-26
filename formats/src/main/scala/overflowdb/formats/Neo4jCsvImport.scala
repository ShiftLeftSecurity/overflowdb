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
  val Neo4jAdminDoc = "https://neo4j.com/docs/operations-manual/current/tools/neo4j-admin/neo4j-admin-import"

  override def runImport(graph: Graph, inputFiles: Seq[Path]): Unit = {
    var importedNodeCount = 0
    groupInputFiles(inputFiles).foreach { case HeaderAndDataFile(headerFile, dataFile) =>
      val ParsedHeaderFile(fileType, columnDefs) = parseHeaderFile(headerFile)
//      fileType match {
//        case FileType.Nodes => ???
//        case FileType.Relationships => ???
//      }

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

  private def parseHeaderFile(headerFile: Path): ParsedHeaderFile = {
    val columnDefs = Using(CSVReader.open(headerFile.toFile)) { headerReader =>
      headerReader.all().headOption.getOrElse(
        throw new AssertionError(s"header file $headerFile is empty"))
    }.get

    val propertyDefs = Map.newBuilder[Int, PropertyDef]
    // will figure out if this is a node or relationship file during parsing
    var fileType: Option[FileType.Value] = None
    columnDefs.zipWithIndex.foreach { case (entry, idx) =>
      val propertyDef = entry match {
        case ":LABEL" =>
          fileType = Option(FileType.Nodes)
          PropertyDef(None, Neo4jValueType.Label)
        case ":TYPE" =>
          fileType = Option(FileType.Relationships)
          PropertyDef(None, Neo4jValueType.Label)
        case s if s.endsWith(":ID") =>
          PropertyDef(None, Neo4jValueType.Id)
        case s if s.endsWith(":START_ID") =>
          PropertyDef(None, Neo4jValueType.StartId)
        case propertyDef if propertyDef.contains(":") =>
          val name :: valueTpe0 :: Nil = propertyDef.split(':').toList
          val isArray = propertyDef.endsWith("[]") // from the docs: "To define an array type, append [] to the type"
          val valueTpe =
            if (isArray) valueTpe0.dropRight(2)
            else valueTpe0
          PropertyDef(Option(name), valueType = Neo4jValueType.withName(valueTpe), isArray)
        case propertyName =>
          PropertyDef(Option(propertyName), valueType = Neo4jValueType.String)
      }
      propertyDefs.addOne((idx, propertyDef))
    }


    val propertyDefsRes = propertyDefs.result()
    fileType match {
      case Some(FileType.Nodes) =>
        assert(propertyDefsRes.values.exists(_.valueType == Neo4jValueType.Id),
          s"no :ID column found in headerFile $headerFile - see format definition in $Neo4jAdminDoc")
        ParsedHeaderFile(FileType.Nodes, propertyDefsRes)
      case Some(FileType.Relationships) =>
        assert(propertyDefsRes.values.exists(_.valueType == Neo4jValueType.StartId),
          s"no :START_ID column found in headerFile $headerFile - see format definition in $Neo4jAdminDoc")
        assert(propertyDefsRes.values.exists(_.valueType == Neo4jValueType.EndId),
          s"no :END column found in headerFile $headerFile - see format definition in $Neo4jAdminDoc")
        ParsedHeaderFile(FileType.Relationships, propertyDefsRes)
      case None =>
        throw new AssertionError(s"unable to determine file type - neither :LABEL (for nodes) nor :TYPE (for relationships) found")
    }

    val result = ParsedHeaderFile(
      fileType.getOrElse(throw new AssertionError(s"unable to determine file type - neither :LABEL (for nodes) nor :TYPE (for relationships) found")),
      propertyDefs.result()
    )
    List(Neo4jValueType.Id, Neo4jValueType.Label).foreach { valueType =>
      assert(result.propertyByColumnIndex.values.find { _.valueType == valueType }.isDefined,
        s"no $valueType column found in headerFile $headerFile - see format definition in $Neo4jAdminDoc")
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
        case PropertyDef(None, Neo4jValueType.Id, _) =>
          id = entry.toInt
        case PropertyDef(None, Neo4jValueType.Label, _) =>
          label = entry
        case PropertyDef(Some(name), valueType, false) =>
          if (entry != "" || valueType == Neo4jValueType.String) {
            val value = parsePropertyValue(entry, valueType)
            properties.addOne(ParsedProperty(name, value))
          }
        case PropertyDef(Some(name), valueType, true) =>
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
  private case class ParsedHeaderFile(fileType: FileType.Value, propertyByColumnIndex: Map[Int, PropertyDef])
  private case class PropertyDef(name: Option[String], valueType: Neo4jValueType.Value, isArray: Boolean = false)
  private case class ParsedProperty(name: String, value: Any)
  private case class ParsedRowData(id: Int, label: String, properties: Seq[ParsedProperty])

  private object FileType extends Enumeration {
    val Nodes = Value
    val Relationships = Value
  }

  object Neo4jValueType extends Enumeration {
    type Neo4jValueType = Value
    // special types for nodes
    val Id = Value(":ID")
    val Label = Value(":LABEL")

    // special types for relationships
    val Type = Value(":TYPE")
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