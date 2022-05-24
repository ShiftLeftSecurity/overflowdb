package overflowdb.formats.dot

import better.files._
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import overflowdb.formats.graphml.{GraphMLExporter, GraphMLImporter}
import overflowdb.testdomains.simple.{FunkyList, SimpleDomain, TestEdge, TestNode}
import overflowdb.util.DiffTool

import scala.jdk.CollectionConverters.{CollectionHasAsScala, IterableHasAsJava}

class DotTests extends AnyWordSpec {

  "Exporter should export valid dot" in {
    val graph = SimpleDomain.newGraph()

    val node2 = graph.addNode(2, TestNode.LABEL, TestNode.STRING_PROPERTY, "stringProp2")
    val node3 = graph.addNode(3, TestNode.LABEL, TestNode.INT_PROPERTY, 13)

    // only allows values defined in FunkyList.funkyWords
    val funkyList = new FunkyList()
    funkyList.add("apoplectic")
    funkyList.add("bucolic")
    val node1 = graph.addNode(1, TestNode.LABEL,
      TestNode.INT_PROPERTY, 11,
      TestNode.STRING_PROPERTY, "<stringProp1>",
      TestNode.STRING_LIST_PROPERTY, List("stringListProp1a", "stringListProp1b").asJava,
      TestNode.FUNKY_LIST_PROPERTY, funkyList,
      TestNode.INT_LIST_PROPERTY, List(21, 31, 41).asJava,
    )

    node1.addEdge(TestEdge.LABEL, node2, TestEdge.LONG_PROPERTY, Long.MaxValue)
    node2.addEdge(TestEdge.LABEL, node3)

    File.usingTemporaryDirectory(getClass.getName) { exportRootDirectory =>
      val exportResult = DotExporter.runExport(graph, exportRootDirectory.pathAsString)
      exportResult.nodeCount shouldBe 3
      exportResult.edgeCount shouldBe 2
      val Seq(exportedFile) = exportResult.files

      better.files.File(exportedFile).contentAsString.trim shouldBe
        s"""digraph {
           |  2 [label=testNode StringProperty="stringProp2"]
           |  3 [label=testNode StringProperty="DEFAULT_STRING_VALUE" IntProperty=13]
           |  1 [label=testNode FunkyListProperty="apoplectic;bucolic" StringProperty="<stringProp1>" StringListProperty="stringListProp1a;stringListProp1b" IntProperty=11 IntListProperty="21;31;41"]
           |}""".stripMargin.trim
    }
  }

}
