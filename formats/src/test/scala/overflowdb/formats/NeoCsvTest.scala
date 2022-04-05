package overflowdb.formats

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import overflowdb.testdomains.simple.SimpleDomain

import java.nio.file.Paths

class Neo4jCsvTests extends AnyWordSpec {
  val neo4jcsvRoot = Paths.get("src/test/resources/neo4jcsv")

  "foo" in {
    val graph = SimpleDomain.newGraph()
    Neo4jCsvImport.runImport(neo4jcsvRoot.resolve("testnodes"), graph)
  }

}
