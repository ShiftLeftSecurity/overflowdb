package overflowdb.formats.graphml

import better.files.File
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import overflowdb.formats.ExportResult
import overflowdb.testdomains.gratefuldead.GratefulDead
import overflowdb.testdomains.simple.{FunkyList, SimpleDomain, TestEdge, TestNode}
import overflowdb.util.DiffTool

import java.nio.file.Paths
import scala.jdk.CollectionConverters.IterableHasAsJava

class GraphMLTests extends AnyWordSpec {

  "import minified gratefuldead graph" in {
    val graph = GratefulDead.newGraph()
    graph.nodeCount() shouldBe 0

    GraphMLImport.runImport(graph, Paths.get("src/test/resources/graphml-small.xml"))
    graph.nodeCount() shouldBe 3
    graph.edgeCount() shouldBe 2

    val node1 = graph.node(1)
    node1.label() shouldBe "song"
    val node340 = node1.out("sungBy").next()
    val node527 = node1.out("writtenBy").next()

    node340.label shouldBe "artist"
    node340.property("name") shouldBe "Garcia"
    node340.out().hasNext shouldBe false
    node340.in().hasNext shouldBe true

    node527.label shouldBe "artist"
    node527.property("name") shouldBe "Bo_Diddley"
    node527.out().hasNext shouldBe false
    node527.in().hasNext shouldBe true

    graph.close()
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
      val ExportResult(nodeCount, edgeCount, exportedFiles0, additionalInfo) = GraphMLExporter.runExport(graph, exportRootDirectory.pathAsString)
      nodeCount shouldBe 3
      edgeCount shouldBe 2
      fail("continue here")
//      val exportedFiles = exportedFiles0.map(_.toFile.toScala)
//      exportedFiles.size shouldBe 6
//      exportedFiles.foreach(_.parent shouldBe exportRootDirectory)
//
//      // assert csv file contents
//      val nodeHeaderFile = fuzzyFindFile(exportedFiles, TestNode.LABEL, HeaderFileSuffix)
//      nodeHeaderFile.contentAsString.trim shouldBe
//        ":ID,:LABEL,FunkyListProperty:string[],IntListProperty:int[],IntProperty:int,StringListProperty:string[],StringProperty:string"
//
//      val nodeDataFileLines = fuzzyFindFile(exportedFiles, TestNode.LABEL, DataFileSuffix).lines.toSeq
//      nodeDataFileLines.size shouldBe 3
//      nodeDataFileLines should contain("2,testNode,,,,,stringProp2")
//      nodeDataFileLines should contain("3,testNode,,,13,,DEFAULT_STRING_VALUE")
//      nodeDataFileLines should contain("1,testNode,apoplectic;bucolic,21;31;41,11,stringListProp1a;stringListProp1b,stringProp1")
//
//      val edgeHeaderFile = fuzzyFindFile(exportedFiles, TestEdge.LABEL, HeaderFileSuffix)
//      edgeHeaderFile.contentAsString.trim shouldBe ":START_ID,:END_ID,:TYPE,longProperty:long"
//
//      val edgeDataFileLines = fuzzyFindFile(exportedFiles, TestEdge.LABEL, DataFileSuffix).lines.toSeq
//      edgeDataFileLines.size shouldBe 2
//      edgeDataFileLines should contain(s"1,2,testEdge,${Long.MaxValue}")
//      edgeDataFileLines should contain(s"2,3,testEdge,${TestEdge.LONG_PROPERTY_DEFAULT}")
//
//      fuzzyFindFile(exportedFiles, TestNode.LABEL, CypherFileSuffix).contentAsString shouldBe
//        """LOAD CSV FROM 'file:/nodes_testNode_data.csv' AS line
//          |CREATE (:testNode {
//          |id: toInteger(line[0]),
//          |FunkyListProperty: toStringList(split(line[2], ";")),
//          |IntListProperty: toIntegerList(split(line[3], ";")),
//          |IntProperty: toInteger(line[4]),
//          |StringListProperty: toStringList(split(line[5], ";")),
//          |StringProperty: line[6]
//          |});
//          |""".stripMargin
//
//      fuzzyFindFile(exportedFiles, TestEdge.LABEL, CypherFileSuffix).contentAsString shouldBe
//        """LOAD CSV FROM 'file:/edges_testEdge_data.csv' AS line
//          |MATCH (a), (b)
//          |WHERE a.id = toInteger(line[0]) AND b.id = toInteger(line[1])
//          |CREATE (a)-[r:testEdge {longProperty: toInteger(line[3])}]->(b);
//          |""".stripMargin
//
//      // import csv into new graph, use difftool for round trip of conversion
//      val graphFromCsv = SimpleDomain.newGraph()
//      Neo4jCsvImporter.runImport(graphFromCsv, exportedFiles.filterNot(_.name.contains(CypherFileSuffix)).map(_.toJava.toPath))
//      val diff = DiffTool.compare(graph, graphFromCsv)
//      withClue(s"original graph and reimport from csv should be completely equal, but there are differences:\n" +
//        diff.asScala.mkString("\n") +
//        "\n") {
//        diff.size shouldBe 0
//      }
    }
  }


}
