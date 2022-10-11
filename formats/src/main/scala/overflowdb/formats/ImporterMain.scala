package overflowdb.formats

import org.slf4j.LoggerFactory
import overflowdb.formats.graphml.GraphMLImporter
import overflowdb.formats.graphson.GraphSONImporter
import overflowdb.formats.neo4jcsv.Neo4jCsvImporter
import overflowdb.{EdgeFactory, Graph, NodeFactory}
import scopt.OParser

import java.io.File
import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.util.Using

/**
 * Base functionality import a given list of input file(s) of various formats into an OverflowDB binary.
 * Because ODB relies on domain specific implementations, specifically the  NodeFactories and EdgeFactories from the
 * domain-specific generated classes (typically generated by by https://github.com/ShiftLeftSecurity/overflowdb-codegen)
 * need to be passed in.
 */
object ImporterMain extends App {
  lazy val logger = LoggerFactory.getLogger(getClass)

  def apply(nodeFactories: Seq[NodeFactory[_]],
            edgeFactories: Seq[EdgeFactory[_]],
            convertPropertyForPersistence: Any => Any = identity): Array[String] => Unit = {
    args =>
      OParser.parse(parser, args, Config(Nil, null, Paths.get("/dev/null")))
        .map { case Config(inputFiles, format, outputFile) =>
          val nonExistent = inputFiles.filterNot(Files.exists(_))
          if (nonExistent.nonEmpty)
            throw new AssertionError(s"given input files $nonExistent do not exist")

          Files.deleteIfExists(outputFile)

          val importer: Importer = format match {
            case Format.Neo4jCsv => Neo4jCsvImporter
            case Format.GraphML => GraphMLImporter
            case Format.GraphSON => GraphSONImporter
          }
          val odbConfig = overflowdb.Config.withoutOverflow.withStorageLocation(outputFile)
          Using.resource(
            Graph.open(
              odbConfig,
              nodeFactories.asJava,
              edgeFactories.asJava,
              convertPropertyForPersistence(_).asInstanceOf[Object])) { graph =>
            logger.info(s"starting import of ${inputFiles.size} files in format=$format into a new overflowdb instance with storagePath=$outputFile")
            importer.runImport(graph, inputFiles)
            logger.info(s"import completed successfully")
          }
        }
  }

  private lazy val builder = OParser.builder[Config]
  private lazy val parser = {
    import builder._
    OParser.sequence(
      programName("odb-import"),
      help("help").text("prints this usage text"),
      opt[String]('f', "format")
        .required()
        .action((x, c) => c.copy(format = Format.byNameLowercase(x)))
    .text(s"import format, one of [${Format.valuesAsStringLowercase.mkString("|")}]"),
      opt[File]('o', "out") // will be able to read a `Path` with scopt 4.0.2+ (once released)
        .required()
        .action((x, c) => c.copy(outputFile = x.toPath))
        .text("output file for overflowdb binary, e.g. out.odb"),
      arg[File]("inputFiles")
        .required()
        .unbounded()
        .action((x, c) => c.copy(inputFiles = c.inputFiles :+ x.toPath))
        .text("input files - must exist and be readable"),
    )
  }

  private case class Config(inputFiles: Seq[Path], format: Format.Value, outputFile: Path)
}
