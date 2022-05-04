package overflowdb.formats

import overflowdb.formats.neo4jcsv.Neo4jCsvExporter

import java.nio.file.Path
import scopt.OParser

object ExportMain extends App {

  val formatsByNameLowercase: Map[String, Format.Value] =
    Format.values.map(format => (format.toString.toLowerCase, format)).toMap

  val builder = OParser.builder[Config]
  val parser = {
    import builder._
    OParser.sequence(
      programName("odb-export"),
      help("help").text("prints this usage text"),
      opt[String]('f', "format")
        .action((x, c) => c.copy(format = formatsByNameLowercase(x)))
        .text(s"export format, one of [${formatsByNameLowercase.keys.toSeq.sorted.mkString("|")}]"),
    )
  }

  OParser.parse(parser, args, Config(Path.of("/dev/null"), null, Path.of("/dev/null"))).map { config =>
    if (!config.inputFile.toFile.exists)
      throw new AssertionError(s"given input file ${config.inputFile} does not exist")

    val exporter: Exporter = config.format match {
      case Format.Neo4jCsv => Neo4jCsvExporter
      case Format.GraphMl => ???
    }

    val graph = ???
    exporter.runExport(graph, config.outputFile)
  }


  case class Config(inputFile: Path, format: Format.Value, outputFile: Path)
}
