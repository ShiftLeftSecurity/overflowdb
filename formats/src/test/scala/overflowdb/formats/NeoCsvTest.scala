package overflowdb.formats

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import overflowdb.testdomains.simple.{SimpleDomain, TestNode}
import testutils.ProjectRoot

import java.nio.file.Paths

class Neo4jCsvTests extends AnyWordSpec {
  val subprojectRoot = ProjectRoot.relativise("formats")
  val neo4jcsvRoot = Paths.get(subprojectRoot, "src/test/resources/neo4jcsv")

  "import from csv" in {
    val csvInputFiles = Seq("testnodes_header.csv", "testnodes.csv").map(neo4jcsvRoot.resolve)

    val graph = SimpleDomain.newGraph()
    Neo4jCsvImport.runImport(graph, csvInputFiles)

    graph.nodeCount() shouldBe 1
    val node1 = graph.node(1).asInstanceOf[TestNode]
    node1.label() shouldBe "testNode"
    node1.intProperty() shouldBe 11
    node1.stringProperty() shouldBe "stringProp1"
  }

}
