package overflowdb.formats.neo4jcsv

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import overflowdb.testdomains.simple.{FunkyList, SimpleDomain, TestEdge, TestNode}
import testutils.ProjectRoot

import java.nio.file.Paths
import scala.jdk.CollectionConverters.{CollectionHasAsScala, IterableHasAsJava}
import better.files._

class Neo4jCsvTests extends AnyWordSpec {
  val subprojectRoot = ProjectRoot.relativise("formats")
  val neo4jcsvRoot = Paths.get(subprojectRoot, "src/test/resources/neo4jcsv")

  "Importer" should {

    "import valid csv" in {
      val csvInputFiles = Seq(
        "testedges_header.csv",
        "testedges.csv",
        "testnodes_header.csv",
        "testnodes.csv",
      ).map(neo4jcsvRoot.resolve)

      val graph = SimpleDomain.newGraph()
      Neo4jCsvImporter.runImport(graph, csvInputFiles)

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
      val edge1 = node1.outE("testEdge").next().asInstanceOf[TestEdge]
      edge1.longProperty shouldBe Long.MaxValue
      edge1.inNode shouldBe node2

      val edge2 = node3.inE("testEdge").next().asInstanceOf[TestEdge]
      edge2.outNode shouldBe node2
    }

    "fail if multiple labels are used (unsupported by overflowdb)" in {
      val csvInputFiles = Seq(
        "unsupported_multiple_labels_header.csv",
        "unsupported_multiple_labels.csv",
      ).map(neo4jcsvRoot.resolve)

      val graph = SimpleDomain.newGraph()
      intercept[NotImplementedError] {
        Neo4jCsvImporter.runImport(graph, csvInputFiles)
      }.getMessage should include("multiple :LABEL columns found")
    }
  }

  "Exporter should export valid csv" in {
    val graph = SimpleDomain.newGraph()

    val node2 = graph.addNode(2, TestNode.LABEL, TestNode.STRING_PROPERTY, "stringProp2")
    val node3 = graph.addNode(3, TestNode.LABEL, TestNode.INT_PROPERTY, 13)

    // only allows values defined in FunkyList.funkyWords
    val funkyList = new FunkyList()
    funkyList.add("apoplectic")
    funkyList.add("bucolic")
    val node1 = graph.addNode(1, TestNode.LABEL,
      TestNode.INT_PROPERTY, 11,
      TestNode.STRING_PROPERTY, "stringProp1",
      TestNode.STRING_LIST_PROPERTY, List("stringListProp1a", "stringListProp1b").asJava,
      TestNode.FUNKY_LIST_PROPERTY, funkyList,
      TestNode.INT_LIST_PROPERTY, List(21, 31, 41).asJava,
    )

    node1.addEdge(TestEdge.LABEL, node2, TestEdge.LONG_PROPERTY, Long.MaxValue)
    node2.addEdge(TestEdge.LABEL, node3)

    File.usingTemporaryDirectory(getClass.getName) { exportRootDirectory =>
      val exportedFiles = Neo4jCsvExporter.runExport(graph, exportRootDirectory.pathAsString).map(_.toFile.toScala)
      exportedFiles.foreach(_.parent shouldBe exportRootDirectory)
      exportedFiles.size shouldBe 4

      // assert csv file contents
      val nodeHeaderFile = exportedFiles.find { file =>
        val relevantPart = file.nameWithoutExtension.toLowerCase
        relevantPart.contains(TestNode.LABEL.toLowerCase) && relevantPart.endsWith("_header")
      }.get
      nodeHeaderFile.contentAsString.trim shouldBe
        ":ID,:LABEL,FunkyListProperty:string[],IntListProperty:int[],IntProperty:int,StringListProperty:string[],StringProperty:string"

      val nodeDataFileLines = exportedFiles.find { file =>
        val relevantPart = file.nameWithoutExtension.toLowerCase
        relevantPart.contains(TestNode.LABEL.toLowerCase) && !relevantPart.endsWith("_header")
      }.get.lines().toSeq
      nodeDataFileLines.size shouldBe 3
      nodeDataFileLines should contain("2,testNode,,,,,stringProp2")
      nodeDataFileLines should contain("3,testNode,,,13,,DEFAULT_STRING_VALUE")
      nodeDataFileLines should contain("1,testNode,apoplectic;bucolic,21;31;41,11,stringListProp1a;stringListProp1b,stringProp1")

      val edgeHeaderFile = exportedFiles.find { file =>
        val relevantPart = file.nameWithoutExtension.toLowerCase
        relevantPart.contains(TestEdge.LABEL.toLowerCase) && relevantPart.endsWith("_header")
      }.get
      edgeHeaderFile.contentAsString.trim shouldBe ":START_ID,:END_ID,:TYPE,longProperty:long"

      val edgeDataFileLines = exportedFiles.find { file =>
        val relevantPart = file.nameWithoutExtension.toLowerCase
        relevantPart.contains(TestEdge.LABEL.toLowerCase) && !relevantPart.endsWith("_header")
      }.get.lines().toSeq
      edgeDataFileLines.size shouldBe 2
      edgeDataFileLines should contain("1,2,testEdge,9223372036854775807")
      edgeDataFileLines should contain("2,3,testEdge,")


      // TODO use difftool for round trip of conversion?
    }
  }

}
