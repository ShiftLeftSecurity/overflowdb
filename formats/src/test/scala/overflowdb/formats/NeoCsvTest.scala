package overflowdb.formats

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import overflowdb.testdomains.simple.{SimpleDomain, TestNode}
import testutils.ProjectRoot

import java.nio.file.Paths
import scala.jdk.CollectionConverters.CollectionHasAsScala

class Neo4jCsvTests extends AnyWordSpec {
  val subprojectRoot = ProjectRoot.relativise("formats")
  val neo4jcsvRoot = Paths.get(subprojectRoot, "src/test/resources/neo4jcsv")

  "import from csv" in {
    val csvInputFiles = Seq(
      "testnodes_header.csv",
      "testnodes.csv",
      "testedges_header.csv",
      "testedges.csv",
    ).map(neo4jcsvRoot.resolve)

    val graph = SimpleDomain.newGraph()
    Neo4jCsvImport.runImport(graph, csvInputFiles)

    graph.nodeCount shouldBe 3

    val node1 = graph.node(1).asInstanceOf[TestNode]
    node1.label shouldBe "testNode"
    node1.intProperty shouldBe 11
    node1.stringProperty shouldBe "stringProp1"
    node1.stringListProperty.asScala.toList shouldBe List("stringListProp1a", "stringListProp1b")
    node1.intListProperty.asScala.toList shouldBe List(21, 31, 41)

    val node2 = graph.node(2).asInstanceOf[TestNode]
    node2.stringProperty shouldBe "stringProp2"

    val node3 = graph.node(3).asInstanceOf[TestNode]
    node3.intProperty shouldBe 13


    graph.edgeCount shouldBe 2
    ???
  }

}
