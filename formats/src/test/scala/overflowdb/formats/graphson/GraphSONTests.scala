package overflowdb.formats.graphson

import better.files.File
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import overflowdb.testdomains.gratefuldead.GratefulDead
import overflowdb.testdomains.simple.{FunkyList, SimpleDomain, TestEdge, TestNode}
import overflowdb.util.DiffTool

import java.lang.System.lineSeparator
import java.nio.file.Paths
import scala.jdk.CollectionConverters.{CollectionHasAsScala, IterableHasAsJava}

class GraphSONTests extends AnyWordSpec {

  "import minified gratefuldead graph" in {
    val graph = GratefulDead.newGraph()
    graph.nodeCount() shouldBe 0

    GraphSONImporter.runImport(graph, Paths.get(getClass.getResource("/graphson-small.json").toURI))
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

  "Exporter should export valid json" when {
    "not using (unsupported) list properties" in {
      val graph = SimpleDomain.newGraph()

      val node2 = graph.addNode(2, TestNode.LABEL, TestNode.STRING_PROPERTY, "stringProp2")
      val node3 = graph.addNode(3, TestNode.LABEL, TestNode.INT_PROPERTY, 13)

      // only allows values defined in FunkyList.funkyWords
      val funkyList = new FunkyList()
      funkyList.add("apoplectic")
      funkyList.add("bucolic")
      val node1 = graph.addNode(1, TestNode.LABEL, TestNode.INT_PROPERTY, 11, TestNode.STRING_PROPERTY, "<stringProp1>")

      node1.addEdge(TestEdge.LABEL, node2, TestEdge.LONG_PROPERTY, Long.MaxValue)
      node2.addEdge(TestEdge.LABEL, node3)

      File.usingTemporaryDirectory(getClass.getName) { exportRootDirectory =>
        val exportResult = GraphSONExporter.runExport(graph, exportRootDirectory.pathAsString)
        exportResult.nodeCount shouldBe 3
        exportResult.edgeCount shouldBe 2
        val Seq(graphMLFile) = exportResult.files

        // import graphml into new graph, use difftool for round trip of conversion
        val reimported = SimpleDomain.newGraph()
        GraphSONImporter.runImport(reimported, graphMLFile)
        val diff = DiffTool.compare(graph, reimported)
        withClue(
          s"original graph and reimport from graphml should be completely equal, but there are differences:\n" +
            diff.asScala.mkString("\n") +
            "\n"
        ) {
          diff.size shouldBe 0
        }
      }
    }

    "using list properties" in {
      val graph = SimpleDomain.newGraph()

      // should not discard the list properties
      graph.addNode(
        1,
        TestNode.LABEL,
        TestNode.INT_PROPERTY,
        11,
        TestNode.STRING_PROPERTY,
        "<stringProp1>",
        TestNode.STRING_LIST_PROPERTY,
        List("stringListProp1a", "stringListProp1b").asJava,
        TestNode.INT_LIST_PROPERTY,
        List(21, 31, 41).asJava
      )

      File.usingTemporaryDirectory(getClass.getName) { exportRootDirectory =>
        val exportResult = GraphSONExporter.runExport(graph, exportRootDirectory.pathAsString)
        exportResult.nodeCount shouldBe 1
        exportResult.edgeCount shouldBe 0
        exportResult.additionalInfo.isEmpty shouldBe true
        val Seq(graphJsonFile) = exportResult.files

        // import graphml into new graph, use difftool for round trip of conversion
        val reimported = SimpleDomain.newGraph()
        GraphSONImporter.runImport(reimported, graphJsonFile)
        val diff = DiffTool.compare(graph, reimported)
        val diffString = diff.asScala.mkString(lineSeparator)
        withClue(
          s"original graph contained two list properties, these should also be present in reimported graph $diffString $lineSeparator"
        ) {
          diff.size shouldBe 0
        }
      }
    }

    "using 'contained node' property" in {
      val graph = SimpleDomain.newGraph()

      val node1 = graph.addNode(1, TestNode.LABEL)
      graph.addNode(2, TestNode.LABEL, TestNode.CONTAINED_TESTNODE_PROPERTY, node1, TestNode.INT_PROPERTY, 11)

      File.usingTemporaryDirectory(getClass.getName) { exportRootDirectory =>
        val exportResult = GraphSONExporter.runExport(graph, exportRootDirectory.pathAsString)
        exportResult.nodeCount shouldBe 2
        val Seq(graphJsonFile) = exportResult.files

        // import graphml into new graph, use difftool for round trip of conversion
        val reimported = SimpleDomain.newGraph()
        GraphSONImporter.runImport(reimported, graphJsonFile)
        val diff = DiffTool.compare(graph, reimported)
        val diffString = diff.asScala.mkString(lineSeparator)
        withClue(
          s"original graph contained two list properties, these should also be present in reimported graph $diffString $lineSeparator"
        ) {
          diff.size shouldBe 0
        }
      }
    }
  }

}
