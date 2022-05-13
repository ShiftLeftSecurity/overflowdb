package overflowdb.formats.neo4jcsv

import better.files._
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import overflowdb.formats.{ExporterMain, ImporterMain}
import overflowdb.testdomains.simple.{FunkyList, SimpleDomain, TestEdge, TestNode}
import overflowdb.traversal._
import overflowdb.util.DiffTool
import testutils.ProjectRoot

import java.io.FileNotFoundException
import java.nio.file.Paths
import scala.jdk.CollectionConverters.{CollectionHasAsScala, IterableHasAsJava}

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

    "fail if input file doesn't exist" in {
      val csvInputFiles = Seq(
        "does_not_exist_header.csv",
        "does_not_exist.csv",
      ).map(neo4jcsvRoot.resolve)

      val graph = SimpleDomain.newGraph()
      intercept[FileNotFoundException] {
        Neo4jCsvImporter.runImport(graph, csvInputFiles)
      }
    }

    "fail with context information (line number etc.) for invalid input" in {
      val csvInputFiles = Seq(
        "invalid_column_content_header.csv",
        "invalid_column_content.csv",
      ).map(neo4jcsvRoot.resolve)

      val graph = SimpleDomain.newGraph()
      intercept[RuntimeException] {
        Neo4jCsvImporter.runImport(graph, csvInputFiles)
      }.getMessage should include("invalid_column_content.csv line 3")
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
      exportedFiles.size shouldBe 5

      // assert csv file contents
      val nodeHeaderFile = fuzzyFindFile(exportedFiles, TestNode.LABEL, HeaderFileSuffix)
      nodeHeaderFile.contentAsString.trim shouldBe
        ":ID,:LABEL,FunkyListProperty:string[],IntListProperty:int[],IntProperty:int,StringListProperty:string[],StringProperty:string"

      val nodeDataFileLines = fuzzyFindFile(exportedFiles, TestNode.LABEL, DataFileSuffix).lines.toSeq
      nodeDataFileLines.size shouldBe 3
      nodeDataFileLines should contain("2,testNode,,,,,stringProp2")
      nodeDataFileLines should contain("3,testNode,,,13,,DEFAULT_STRING_VALUE")
      nodeDataFileLines should contain("1,testNode,apoplectic;bucolic,21;31;41,11,stringListProp1a;stringListProp1b,stringProp1")

      val edgeHeaderFile = fuzzyFindFile(exportedFiles, TestEdge.LABEL, HeaderFileSuffix)
      edgeHeaderFile.contentAsString.trim shouldBe ":START_ID,:END_ID,:TYPE,longProperty:long"

      val edgeDataFileLines = fuzzyFindFile(exportedFiles, TestEdge.LABEL, DataFileSuffix).lines.toSeq
      edgeDataFileLines.size shouldBe 2
      edgeDataFileLines should contain(s"1,2,testEdge,${Long.MaxValue}")
      edgeDataFileLines should contain(s"2,3,testEdge,${TestEdge.LONG_PROPERTY_DEFAULT}")

      val cypherFileContent = fuzzyFindFile(exportedFiles, TestNode.LABEL, CypherFileSuffix).contentAsString
      println(cypherFileContent) // TODO drop println
      cypherFileContent shouldBe
        """LOAD CSV FROM 'file:/nodes_testNode_data.csv' AS line
          |CREATE (:testNode {
          |id: line[0],
          |FunkyListProperty: split(line[2], ";"),
          |IntListProperty: toIntegerList(split(line[3]), ";"),
          |IntProperty: toInteger(line[4]),
          |StringListProperty: split(line[5], ";"),
          |StringProperty: line[6]
          |});
          |""".stripMargin
      println(nodeDataFileLines.mkString("\n"))

      // TODO same for TestEdge.LABEL/cypher
      ???

      // import csv into new graph, use difftool for round trip of conversion
      val graphFromCsv = SimpleDomain.newGraph()
      Neo4jCsvImporter.runImport(graphFromCsv, exportedFiles.map(_.toJava.toPath))
      val diff = DiffTool.compare(graph, graphFromCsv)
      withClue(s"original graph and reimport from csv should be completely equal, but there are differences:\n" +
        diff.asScala.mkString("\n") +
        "\n") {
        diff.size shouldBe 0
      }
    }
  }

  "main apps for cli export/import" in {
    File.usingTemporaryDirectory(getClass.getName) { tmpDir =>
      val graphPath = tmpDir/"original.odb"
      val exportPath = tmpDir/"export"
      val graph = SimpleDomain.newGraph(overflowdb.Config.withoutOverflow().withStorageLocation(graphPath.toJava.toPath))
      val node2 = graph.addNode(2, TestNode.LABEL, TestNode.STRING_PROPERTY, "stringProp2")
      val node3 = graph.addNode(3, TestNode.LABEL, TestNode.INT_PROPERTY, 13)
      node2.addEdge(TestEdge.LABEL, node3)
      graph.close()

      val exporterMain = ExporterMain(Seq(TestNode.factory), Seq(TestEdge.factory))
      exporterMain(Array("--format=neo4jcsv", s"--out=${exportPath.pathAsString}", graphPath.pathAsString))
      val exportedFiles = exportPath.list.toArray
      exportedFiles.size shouldBe 4

      // use importer for round trip
      val importerMain = ImporterMain(Seq(TestNode.factory), Seq(TestEdge.factory))
      val reimportPath = tmpDir/"reimported.odb"
      importerMain(Array("--format=neo4jcsv", s"--out=${reimportPath.pathAsString}") ++ exportedFiles.map(_.pathAsString))
      val graphReimported = SimpleDomain.newGraph(overflowdb.Config.withoutOverflow().withStorageLocation(reimportPath.toJava.toPath))
      graphReimported.nodeCount shouldBe 2
      graphReimported.node(2).out(TestEdge.LABEL).property(TestNode.INT_PROPERTY).l shouldBe Seq(13)
    }
  }

  private def fuzzyFindFile(files: Seq[File], label: String, fileSuffix: String): File = {
    files.find { file =>
      val relevantPart = file.nameWithoutExtension.toLowerCase
      relevantPart.contains(label.toLowerCase) && relevantPart.endsWith(fileSuffix)
    }.get
  }

}
