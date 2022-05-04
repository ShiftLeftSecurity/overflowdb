package overflowdb.formats

import overflowdb.formats.neo4jcsv.Neo4jCsvImporter
import overflowdb.{EdgeFactory, Graph, NodeFactory}
import scopt.OParser

import java.io.File
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.util.Using

/**
 * Main class to import a given list of input file(s) of various formats into an OverflowDB binary.
 * Because ODB relies on domain specific implementations, this is an abstract class - to use it, we need the
 * NodeFactories and EdgeFactories of the specific domain implementation, e.g. from the classes generated
 * by https://github.com/ShiftLeftSecurity/overflowdb-codegen
 */
abstract class ImporterMainBase extends App {
  // abstract members
  def nodeFactories: Seq[NodeFactory[_]]
  def edgeFactories: Seq[EdgeFactory[_]]

  val formatsByNameLowercase: Map[String, Format.Value] =
    Format.values.map(format => (format.toString.toLowerCase, format)).toMap

  val builder = OParser.builder[Config]
  val parser = {
    import builder._
    OParser.sequence(
      programName("odb-import"),
      help("help").text("prints this usage text"),
      opt[String]('f', "format")
        .required
        .action((x, c) => c.copy(format = formatsByNameLowercase(x)))
        .text(s"import format, one of [${formatsByNameLowercase.keys.toSeq.sorted.mkString("|")}]"),
      opt[File]('o', "out") // will be able to read a `Path` with scopt 4.0.2+ (once released)
        .required
        .action((x, c) => c.copy(outputFile = x.toPath))
        .text("output file or directory - must exist and be writable"),
      arg[File]("inputFiles")
        .required
        .unbounded
        .action((x, c) => c.copy(inputFiles = c.inputFiles :+ x.toPath))
        .text("input files - must exist and be readable"),
    )
  }

  OParser.parse(parser, args, Config(Nil, null, Path.of("/dev/null")))
    .map { case Config(inputFiles, format, outputFile) =>
      val nonExistent = inputFiles.filterNot(Files.exists(_))
      if (nonExistent.nonEmpty)
        throw new AssertionError(s"given input files $nonExistent do not exist")

      Files.deleteIfExists(outputFile)

      val importer: Importer = format match {
        case Format.Neo4jCsv => Neo4jCsvImporter
        case Format.GraphMl => GraphMLImport
      }
      val odbConfig = overflowdb.Config.withoutOverflow.withStorageLocation(outputFile)
      Using.resource(Graph.open(odbConfig, nodeFactories.asJava, edgeFactories.asJava)) { graph =>
        importer.runImport(graph, inputFiles)
      }
  }

  case class Config(inputFiles: Seq[Path], format: Format.Value, outputFile: Path)
}
