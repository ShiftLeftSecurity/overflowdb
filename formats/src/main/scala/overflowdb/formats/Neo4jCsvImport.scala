package overflowdb.formats

import com.github.tototoshi.csv._
import overflowdb.Graph
import java.nio.file.Path

object Neo4jCsvImport extends Importer {

  override def runImport(graph: Graph, inputFiles: Seq[Path]): Unit = {
    ???
  }

}
