package overflowdb.formats

import overflowdb.{EdgeFactory, Graph, NodeFactory}
import overflowdb.formats.neo4jcsv.Neo4jCsvExporter

import java.nio.file.{Files, Path}
import scopt.OParser

import java.io.File
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.util.Using

/**
 * Main class to export a given OverflowDB graph to various export formats.
 * Because ODB relies on domain specific implementations, this is an abstract class - to use it, we need the
 * NodeFactories and EdgeFactories of the specific domain implementation, e.g. from the classes generated
 * by https://github.com/ShiftLeftSecurity/overflowdb-codegen
 */
abstract class ExporterMainBase extends App {
  // abstract members
  def nodeFactories: Seq[NodeFactory[_]]
  def edgeFactories: Seq[EdgeFactory[_]]

  val formatsByNameLowercase: Map[String, Format.Value] =
    Format.values.map(format => (format.toString.toLowerCase, format)).toMap

  val builder = OParser.builder[Config]
  val parser = {
    import builder._
    OParser.sequence(
      programName("odb-export"),
      help("help").text("prints this usage text"),
      opt[String]('f', "format")
        .required
        .action((x, c) => c.copy(format = formatsByNameLowercase(x)))
        .text(s"export format, one of [${formatsByNameLowercase.keys.toSeq.sorted.mkString("|")}]"),
      opt[File]('o', "out") // will be able to read a `Path` with scopt 4.0.2+ (once released)
        .required
        .action((x, c) => c.copy(outputFile = x.toPath))
        .text("output file or directory - must exist and be writable"),
      arg[File]("odbBinaryFile")
        .required()
        .action((x, c) => c.copy(inputFile = x.toPath))
        .text("input overflowdb graph file - must exist and be readable"),
    )
  }

  OParser.parse(parser, args, Config(Path.of("/dev/null"), null, Path.of("/dev/null")))
    .map { case Config(inputFile, format, outputFile) =>
      if (!inputFile.toFile.exists)
        throw new AssertionError(s"given input file $inputFile does not exist")
      if (outputFile.toFile.exists) {
        if (!outputFile.toFile.isDirectory)
          throw new AssertionError(s"output file $outputFile already exists and is not a directory")
      } else {
        Files.createDirectories(outputFile)
      }

      val exporter: Exporter = format match {
        case Format.Neo4jCsv => Neo4jCsvExporter
        case Format.GraphMl => ???
      }
      val odbConfig = overflowdb.Config.withoutOverflow.withStorageLocation(inputFile)
      Using(Graph.open(odbConfig, nodeFactories.asJava, edgeFactories.asJava)) { graph =>
        exporter.runExport(graph, outputFile)
      }.get
  }

  case class Config(inputFile: Path, format: Format.Value, outputFile: Path)
}
