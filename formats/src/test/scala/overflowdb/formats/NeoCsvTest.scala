package overflowdb.formats

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import overflowdb.testdomains.simple.SimpleDomain

import java.nio.file.Paths

class Neo4jCsvTests extends AnyWordSpec {
  val neo4jcsvRoot = Paths.get("src/test/resources/neo4jcsv")

  "import from csv" in {
    val csvInputFiles = Seq("testnodes_header.csv", "testnodes.csv").map(neo4jcsvRoot.resolve)

    val graph = SimpleDomain.newGraph()
    Neo4jCsvImport.runImport(graph, csvInputFiles)

    graph.nodeCount() shouldBe 1
  }

}
