package overflowdb.formats

import overflowdb.Graph

import java.nio.file.Path

trait Importer {

  def run(inputFile: Path, graph: Graph): Unit

  def run(inputFile: String, graph: Graph): Unit =
    run(Path.of(inputFile), graph)

}
