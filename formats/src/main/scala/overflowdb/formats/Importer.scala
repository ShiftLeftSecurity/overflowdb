package overflowdb.formats

import overflowdb.Graph

import java.nio.file.Path

trait Importer {

  def runImport(inputFile: Path, graph: Graph): Unit

  def runImport(inputFile: String, graph: Graph): Unit =
    runImport(Path.of(inputFile), graph)

}
