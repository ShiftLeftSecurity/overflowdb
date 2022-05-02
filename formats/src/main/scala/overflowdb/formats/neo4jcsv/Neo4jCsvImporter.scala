package overflowdb.formats.neo4jcsv

import com.github.tototoshi.csv._
import overflowdb.Graph
import overflowdb.formats.Importer

import java.nio.file.Path
import java.util
import scala.util.Using

/**
 * Imports from neo4j csv files
 * see https://neo4j.com/docs/operations-manual/current/tools/neo4j-admin/neo4j-admin-import/
 * */
object Neo4jCsvImporter extends Importer {
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

    val (headerFiles, dataFiles) = inputFiles.partition(_.getFileName.toString.endsWith(s"$HeaderFileSuffix.csv"))
    assert(headerFiles.size == dataFiles.size,
      s"number of header files (`xyz$HeaderFileSuffix.csv`) must equal the number of data files (`xyz.csv`)")

    headerFiles.map { headerFile =>
      val wantedBodyFilename = headerFile.getFileName.toString.replaceAll(HeaderFileSuffix, "")
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

    val propertyDefs = Map.newBuilder[Int, ColumnDef]
    var labelColumnFound = false
    // will figure out if this is a node or relationship file during parsing
    var fileType: Option[FileType.Value] = None
    columnDefs.zipWithIndex.foreach { case (entry, idx) =>
      val propertyDef = entry match {
        case ColumnType.LabelMarker  =>
          if (labelColumnFound)
            throw new NotImplementedError(s"multiple ${ColumnType.Label} columns found in $headerFile, which is not supported by overflowdb")
          labelColumnFound = true
          fileType = Option(FileType.Nodes)
          ColumnDef(None, ColumnType.Label)
        case ColumnType.TypeMarker =>
          fileType = Option(FileType.Relationships)
          ColumnDef(None, ColumnType.Type)
        case s if s.endsWith(ColumnType.Id.toString) =>
          ColumnDef(None, ColumnType.Id)
        case s if s.endsWith(ColumnType.StartId.toString) =>
          ColumnDef(None, ColumnType.StartId)
        case s if s.endsWith(ColumnType.EndId.toString) =>
          ColumnDef(None, ColumnType.EndId)
        case propertyDef if propertyDef.contains(":") =>
          val name :: valueTpe0 :: Nil = propertyDef.split(':').toList
          val isArray = propertyDef.endsWith(ColumnType.ArrayMarker) // from the docs: "To define an array type, append [] to the type"
          val valueTpe =
            if (isArray) valueTpe0.dropRight(2)
            else valueTpe0
          ColumnDef(Option(name), valueType = ColumnType.withName(valueTpe), isArray)
        case propertyName =>
          ColumnDef(Option(propertyName), valueType = ColumnType.String) // if property is not annotated with `:someType`, default to String
      }
      propertyDefs.addOne((idx, propertyDef))
    }

    val propertyDefsRes = propertyDefs.result()
    fileType match {
      case Some(FileType.Nodes) =>
        assert(propertyDefsRes.values.exists(_.valueType == ColumnType.Id),
          s"no :ID column found in headerFile $headerFile - see format definition in $Neo4jAdminDoc")
        ParsedHeaderFile(FileType.Nodes, propertyDefsRes)
      case Some(FileType.Relationships) =>
        assert(propertyDefsRes.values.exists(_.valueType == ColumnType.StartId),
          s"no :START_ID column found in headerFile $headerFile - see format definition in $Neo4jAdminDoc")
        assert(propertyDefsRes.values.exists(_.valueType == ColumnType.EndId),
          s"no :END_ID column found in headerFile $headerFile - see format definition in $Neo4jAdminDoc")
        ParsedHeaderFile(FileType.Relationships, propertyDefsRes)
      case _ =>
        throw new AssertionError(s"unable to determine file type - neither ${ColumnType.Label} (for nodes) nor ${ColumnType.Type} (for relationships) found")
    }
  }

  private def parseNodeRowData(columnsRaw: Seq[String], lineNo: Int, columnDefs: Map[Int, ColumnDef]): ParsedNodeRowData = {
    var id: Integer = null
    var label: String = null
    val properties = Seq.newBuilder[ParsedProperty]
    columnsRaw.zipWithIndex.foreach { case (entry, idx) =>
      assert(columnDefs.contains(idx), s"column with index=$idx not found in column definitions derived from headerFile")
      columnDefs(idx) match {
        case ColumnDef(_, ColumnType.Id, _) =>
          id = entry.toInt
        case ColumnDef(_, ColumnType.Label, _) =>
          label = entry
        case ColumnDef(Some(name), valueType, false) =>
          parseProperty(entry, name, valueType).foreach(properties.addOne)
        case ColumnDef(Some(name), valueType, true) =>
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

  private def parseEdgeRowData(columnsRaw: Seq[String], lineNo: Int, columnDefs: Map[Int, ColumnDef]): ParsedEdgeRowData = {
    var startId: Integer = null
    var endId: Integer = null
    var label: String = null
    val properties = Seq.newBuilder[ParsedProperty]
    columnsRaw.zipWithIndex.foreach { case (entry, idx) =>
      assert(columnDefs.contains(idx), s"column with index=$idx not found in column definitions derived from headerFile")
      columnDefs(idx) match {
        case ColumnDef(_, ColumnType.StartId, _) =>
          startId = entry.toInt
        case ColumnDef(_, ColumnType.EndId, _) =>
          endId = entry.toInt
        case ColumnDef(_, ColumnType.Type, _) =>
          label = entry
        case ColumnDef(Some(name), valueType, false) =>
          parseProperty(entry, name, valueType).foreach(properties.addOne)
        case ColumnDef(Some(name), valueType, true) =>
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

  private def parseProperty(rawValue: String, name: String, valueType: ColumnType.Value): Option[ParsedProperty] = {
    if (rawValue != "" || valueType == ColumnType.String)
      Some(ParsedProperty(name, parsePropertyValue(rawValue, valueType)))
    else
      None
  }

  private def parseArrayProperty(rawValue: String, name: String, valueType: ColumnType.Value): Option[ParsedProperty] = {
    val values = rawValue.split(';') // from the docs: "By default, array values are separated by ;"
    if (values.nonEmpty && values.head != "") { // csv parser always adds one empty string entry...
      val parsedValues = values.map(parsePropertyValue(_, valueType))
      Some(ParsedProperty(name, parsedValues))
    } else {
      None
    }
  }

  private def parsePropertyValue(rawString: String, valueType: ColumnType.Value): Any = {
    valueType match {
      case ColumnType.Int => rawString.toInt
      case ColumnType.Long => rawString.toLong
      case ColumnType.Float => rawString.toFloat
      case ColumnType.Double => rawString.toDouble
      case ColumnType.Boolean => rawString.toBoolean
      case ColumnType.Byte => rawString.toByte
      case ColumnType.Short => rawString.toShort
      case ColumnType.Char => rawString.head
      case ColumnType.String => rawString
      case ColumnType.Point => ???
      case ColumnType.Date => ???
      case ColumnType.LocalTime => ???
      case ColumnType.Time => ???
      case ColumnType.LocalDateTime => ???
      case ColumnType.DateTime => ???
      case ColumnType.Duration => ???
    }
  }

  private case class HeaderAndDataFile(headerFile: ParsedHeaderFile, dataFile: Path)
  private case class ParsedHeaderFile(fileType: FileType.Value, propertyByColumnIndex: Map[Int, ColumnDef])
  private case class ColumnDef(name: Option[String], valueType: ColumnType.Value, isArray: Boolean = false)
  private case class ParsedProperty(name: String, value: Any)
  private case class ParsedNodeRowData(id: Int, label: String, properties: Seq[ParsedProperty])
  private case class ParsedEdgeRowData(startId: Int, endId: Int, label: String, properties: Seq[ParsedProperty])

}
