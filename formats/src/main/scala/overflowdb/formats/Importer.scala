package overflowdb.formats

import org.slf4j.LoggerFactory
import overflowdb.{BatchedUpdate, Graph}

import java.nio.file.{Path, Paths}

trait Importer {
  protected val logger = LoggerFactory.getLogger(getClass)

  def createDiffGraph(inputFiles: Seq[Path]): BatchedUpdate.DiffGraph

  def runImport(graph: Graph, inputFiles: Seq[Path]): Unit = {
    val diffGraph = createDiffGraph(inputFiles)
    BatchedUpdate.applyDiff(graph, diffGraph)
  }

  def runImport(graph: Graph, inputFile: Path): Unit =
    runImport(graph, Seq(inputFile))

  def runImport(graph: Graph, inputFile: String): Unit =
    runImport(graph, Paths.get(inputFile))

}
