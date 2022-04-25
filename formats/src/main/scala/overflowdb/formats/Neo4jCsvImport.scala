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
    groupInputFiles(inputFiles).foreach { case HeaderAndDataFile(headerFile, dataFile) =>
      val columnDefs = parseHeaderFile(headerFile)

      // TODO refactor: extract to method
      Using(CSVReader.open(dataFile.toFile)) { dataReader =>
        dataReader.foreach { columns =>
          assert(columns.size == columnDefs.size, s"datafile row must have the same column count as the headerfile (${columnDefs.size}) - instead found ${columns.size} for row=${columns.mkString(",")}")

          columns.zipWithIndex.foreach { case (entry, idx) =>
            val columnDef = columnDefs.get(idx)
              .getOrElse(throw new AssertionError(s"column with index=$idx not found in column definitions derived from headerFile $headerFile"))


            println(s"$entry $columnDef")
          }
        }
      }
    }
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
          val name :: valueTpe :: Nil = propertyDef.split(':').toList
          PropertyDef(name, valueType = Neo4jValueType.withName(valueTpe))
        case propertyName =>
          PropertyDef(propertyName, valueType = Neo4jValueType.String)
      }
      propertyDefs.addOne((idx, propertyDef))
    }

    val result = propertyDefs.result()
    List(Neo4jValueType.Id, Neo4jValueType.Label).foreach { valueType =>
      assert(result.find(_._2.valueType == valueType).isDefined,
        s"no $valueType column found in headerFile $headerFile - see format definition in https://neo4j.com/docs/operations-manual/current/tools/neo4j-admin/neo4j-admin-import/#import-tool-header-format")
    }
    result
  }

  private case class HeaderAndDataFile(headerFile: Path, dataFile: Path)
  private case class PropertyDef(name: String, valueType: Neo4jValueType.Value)

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