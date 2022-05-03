package overflowdb.formats

import java.nio.file.Path

object ExportMain extends App {
  

  case class Config(inputFile: Path, format: Format.Value, outputFile: Path)
}
