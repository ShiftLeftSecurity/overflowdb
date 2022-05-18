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

    GraphMLImporter.runImport(graph, Paths.get("src/test/resources/graphml-small.xml"))
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
      // TODO add lists back in
//      TestNode.STRING_LIST_PROPERTY, List("stringListProp1a", "stringListProp1b").asJava,
//      TestNode.FUNKY_LIST_PROPERTY, funkyList,
//      TestNode.INT_LIST_PROPERTY, List(21, 31, 41).asJava,
    )

    node1.addEdge(TestEdge.LABEL, node2, TestEdge.LONG_PROPERTY, Long.MaxValue)
    node2.addEdge(TestEdge.LABEL, node3)

    File.usingTemporaryDirectory(getClass.getName) { exportRootDirectory =>
      val exportResult = GraphMLExporter.runExport(graph, exportRootDirectory.pathAsString)
      exportResult.nodeCount shouldBe 3
      exportResult.edgeCount shouldBe 2
      val graphMLFile = Seq(exportResult.files)

      // import graphml into new graph, use difftool for round trip of conversion
      // TODO
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
