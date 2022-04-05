package overflowdb.formats

import com.github.tototoshi.csv._
import overflowdb.Graph

import java.nio.file.Path
import scala.util.Using

/**
 * imports neo4j csv files,
 * see https://neo4j.com/docs/operations-manual/current/tools/neo4j-admin/neo4j-admin-import/
 * */
object Neo4jCsvImport extends Importer {

  override def runImport(graph: Graph, inputFiles: Seq[Path]): Unit = {
    groupInputFiles(inputFiles).foreach { case HeaderAndDataFile(headerFile, dataFile) =>
      Using.resources(CSVReader.open(headerFile.toFile), CSVReader.open(dataFile.toFile)) { (headerReader, dataReader) =>
        val firstLine = headerReader.all().headOption.getOrElse(
          throw new AssertionError(s"header file $headerFile is empty"))
        // TODO parse header format


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

  private case class HeaderAndDataFile(headerFile: Path, dataFile: Path)
}
