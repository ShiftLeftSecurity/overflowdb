package overflowdb.formats

import com.github.tototoshi.csv._
import overflowdb.Graph

import java.nio.file.Path
import java.util
import scala.util.Using

/**
 * Imports from neo4j csv files
 * see https://neo4j.com/docs/operations-manual/current/tools/neo4j-admin/neo4j-admin-import/
 * */
object Neo4jCsvImport extends Importer {
  val Neo4jAdminDoc = "https://neo4j.com/docs/operations-manual/current/tools/neo4j-admin/neo4j-admin-import"

  override def runImport(graph: Graph, inputFiles: Seq[Path]): Unit = {
    var importedNodeCount = 0
    var importedEdgeCount = 0

    groupInputFiles(inputFiles)
      .sortBy(nodeFilesFirst)
      .foreach { case HeaderAndDataFile(ParsedHeaderFile(fileType, columnDefs), dataFile) =>
        Using(CSVReader.open(dataFile.toFile)) { dataReader =>
          dataReader.iterator.zipWithIndex.foreach { case (columnsRaw, idx) =>
            assert(columnsRaw.size == columnDefs.size, s"datafile row must have the same column count as the headerfile (${columnDefs.size}) - instead found ${columnsRaw.size} for row=${columnsRaw.mkString(",")}")
            val lineNo = idx + 1
            fileType match {
              case FileType.Nodes =>
                parseNodeRowData(columnsRaw, lineNo, columnDefs) match {
                  case ParsedNodeRowData(id, label, properties) =>
                    val propertiesAsKeyValues = properties.flatMap(parsedProperty => Seq(parsedProperty.name, parsedProperty.value))
                    graph.addNode(id, label, propertiesAsKeyValues: _*)
                    importedNodeCount += 1
                }
              case FileType.Relationships =>
                parseEdgeRowData(columnsRaw, lineNo, columnDefs) match {
                  case ParsedEdgeRowData(startId, endId, label, properties) =>
                    val startNode = graph.node(startId)
                    val endNode = graph.node(endId)
                    val propertiesMap = new util.HashMap[String, Object]
                    properties.foreach { case ParsedProperty(name, value) =>
                      propertiesMap.put(name, value.asInstanceOf[Object])
                    }
                    startNode.addEdge(label, endNode, propertiesMap)
                    importedEdgeCount += 1
                }
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
        case Some(dataFile) =>
          HeaderAndDataFile(parseHeaderFile(headerFile), dataFile)
        case None =>
          val inputFilenames = inputFiles.map(_.getFileName).mkString(", ")
          throw new AssertionError(s"body filename `$wantedBodyFilename` wanted, but not found in given input files: $inputFilenames")
      }
    }
  }

  private def nodeFilesFirst(headerAndDataFile: HeaderAndDataFile): Int = {
    headerAndDataFile.headerFile.fileType match {
      case FileType.Nodes => 1 // we must import nodes first, because relationships refer to nodes
      case FileType.Relationships => 2
    }
  }

  private def parseHeaderFile(headerFile: Path): ParsedHeaderFile = {
    val columnDefs = Using(CSVReader.open(headerFile.toFile)) { headerReader =>
      headerReader.all().headOption.getOrElse(
        throw new AssertionError(s"header file $headerFile is empty"))
    }.get

    val propertyDefs = Map.newBuilder[Int, CsvColumnDef]
    var labelColumnFound = false
    // will figure out if this is a node or relationship file during parsing
    var fileType: Option[FileType.Value] = None
    columnDefs.zipWithIndex.foreach { case (entry, idx) =>
      val propertyDef = entry match {
        case ":LABEL" =>
          if (labelColumnFound)
            throw new NotImplementedError(s"multiple :LABEL columns found in $headerFile, which is not supported by overflowdb")
          labelColumnFound = true
          fileType = Option(FileType.Nodes)
          CsvColumnDef(None, CsvColumnType.Label)
        case ":TYPE" =>
          fileType = Option(FileType.Relationships)
          CsvColumnDef(None, CsvColumnType.Type)
        case s if s.endsWith(":ID") =>
          CsvColumnDef(None, CsvColumnType.Id)
        case s if s.endsWith(":START_ID") =>
          CsvColumnDef(None, CsvColumnType.StartId)
        case s if s.endsWith(":END_ID") =>
          CsvColumnDef(None, CsvColumnType.EndId)
        case propertyDef if propertyDef.contains(":") =>
          val name :: valueTpe0 :: Nil = propertyDef.split(':').toList
          val isArray = propertyDef.endsWith("[]") // from the docs: "To define an array type, append [] to the type"
          val valueTpe =
            if (isArray) valueTpe0.dropRight(2)
            else valueTpe0
          CsvColumnDef(Option(name), valueType = CsvColumnType.withName(valueTpe), isArray)
        case propertyName =>
          CsvColumnDef(Option(propertyName), valueType = CsvColumnType.String)
      }
      propertyDefs.addOne((idx, propertyDef))
    }

    val propertyDefsRes = propertyDefs.result()
    fileType match {
      case Some(FileType.Nodes) =>
        assert(propertyDefsRes.values.exists(_.valueType == CsvColumnType.Id),
          s"no :ID column found in headerFile $headerFile - see format definition in $Neo4jAdminDoc")
        ParsedHeaderFile(FileType.Nodes, propertyDefsRes)
      case Some(FileType.Relationships) =>
        assert(propertyDefsRes.values.exists(_.valueType == CsvColumnType.StartId),
          s"no :START_ID column found in headerFile $headerFile - see format definition in $Neo4jAdminDoc")
        assert(propertyDefsRes.values.exists(_.valueType == CsvColumnType.EndId),
          s"no :END_ID column found in headerFile $headerFile - see format definition in $Neo4jAdminDoc")
        ParsedHeaderFile(FileType.Relationships, propertyDefsRes)
      case _ =>
        throw new AssertionError(s"unable to determine file type - neither :LABEL (for nodes) nor :TYPE (for relationships) found")
    }
  }

  private def parseNodeRowData(columnsRaw: Seq[String], lineNo: Int, columnDefs: Map[Int, CsvColumnDef]): ParsedNodeRowData = {
    var id: Integer = null
    var label: String = null
    val properties = Seq.newBuilder[ParsedProperty]
    columnsRaw.zipWithIndex.foreach { case (entry, idx) =>
      assert(columnDefs.contains(idx), s"column with index=$idx not found in column definitions derived from headerFile")
      columnDefs(idx) match {
        case CsvColumnDef(_, CsvColumnType.Id, _) =>
          id = entry.toInt
        case CsvColumnDef(_, CsvColumnType.Label, _) =>
          label = entry
        case CsvColumnDef(Some(name), valueType, false) =>
          parseProperty(entry, name, valueType).foreach(properties.addOne)
        case CsvColumnDef(Some(name), valueType, true) =>
          parseArrayProperty(entry, name, valueType).foreach(properties.addOne)
        case other =>
          throw new MatchError(s"unhandled case $other")
      }
    }
    assert(id != null, s"no ID column found in line $lineNo")
    assert(label != null, s"no LABEL column found in line $lineNo")

    val ret = ParsedNodeRowData(id, label, properties.result())
    logger.debug("parsed line {}: {}", lineNo, ret)
    ret
  }

  private def parseEdgeRowData(columnsRaw: Seq[String], lineNo: Int, columnDefs: Map[Int, CsvColumnDef]): ParsedEdgeRowData = {
    var startId: Integer = null
    var endId: Integer = null
    var label: String = null
    val properties = Seq.newBuilder[ParsedProperty]
    columnsRaw.zipWithIndex.foreach { case (entry, idx) =>
      assert(columnDefs.contains(idx), s"column with index=$idx not found in column definitions derived from headerFile")
      columnDefs(idx) match {
        case CsvColumnDef(_, CsvColumnType.StartId, _) =>
          startId = entry.toInt
        case CsvColumnDef(_, CsvColumnType.EndId, _) =>
          endId = entry.toInt
        case CsvColumnDef(_, CsvColumnType.Type, _) =>
          label = entry
        case CsvColumnDef(Some(name), valueType, false) =>
          parseProperty(entry, name, valueType).foreach(properties.addOne)
        case CsvColumnDef(Some(name), valueType, true) =>
          parseArrayProperty(entry, name, valueType).foreach(properties.addOne)
        case other =>
          throw new MatchError(s"unhandled case $other")
      }
    }
    assert(startId != null, s"no START_ID column found in line $lineNo")
    assert(endId != null, s"no END_ID column found in line $lineNo")
    assert(label != null, s"no LABEL column found in line $lineNo")

    val ret = ParsedEdgeRowData(startId, endId, label, properties.result())
    logger.debug("parsed line {}: {}", lineNo, ret)
    ret
  }

  private def parseProperty(rawValue: String, name: String, valueType: CsvColumnType.Value): Option[ParsedProperty] = {
    if (rawValue != "" || valueType == CsvColumnType.String)
      Some(ParsedProperty(name, parsePropertyValue(rawValue, valueType)))
    else
      None
  }

  private def parseArrayProperty(rawValue: String, name: String, valueType: CsvColumnType.Value): Option[ParsedProperty] = {
    val values = rawValue.split(';') // from the docs: "By default, array values are separated by ;"
    if (values.nonEmpty && values.head != "") { // csv parser always adds one empty string entry...
      val parsedValues = values.map(parsePropertyValue(_, valueType))
      Some(ParsedProperty(name, parsedValues))
    } else {
      None
    }
  }

  private def parsePropertyValue(rawString: String, valueType: CsvColumnType.Value): Any = {
    valueType match {
      case CsvColumnType.Int => rawString.toInt
      case CsvColumnType.Long => rawString.toLong
      case CsvColumnType.Float => rawString.toFloat
      case CsvColumnType.Double => rawString.toDouble
      case CsvColumnType.Boolean => rawString.toBoolean
      case CsvColumnType.Byte => rawString.toByte
      case CsvColumnType.Short => rawString.toShort
      case CsvColumnType.Char => rawString.head
      case CsvColumnType.String => rawString
      case CsvColumnType.Point => ???
      case CsvColumnType.Date => ???
      case CsvColumnType.LocalTime => ???
      case CsvColumnType.Time => ???
      case CsvColumnType.LocalDateTime => ???
      case CsvColumnType.DateTime => ???
      case CsvColumnType.Duration => ???
    }
  }

  private case class HeaderAndDataFile(headerFile: ParsedHeaderFile, dataFile: Path)
  private case class ParsedHeaderFile(fileType: FileType.Value, propertyByColumnIndex: Map[Int, CsvColumnDef])
  private case class CsvColumnDef(name: Option[String], valueType: CsvColumnType.Value, isArray: Boolean = false)
  private case class ParsedProperty(name: String, value: Any)
  private case class ParsedNodeRowData(id: Int, label: String, properties: Seq[ParsedProperty])
  private case class ParsedEdgeRowData(startId: Int, endId: Int, label: String, properties: Seq[ParsedProperty])

  private object FileType extends Enumeration {
    val Nodes = Value
    val Relationships = Value
  }

  object CsvColumnType extends Enumeration {
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