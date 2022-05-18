package overflowdb.formats.graphml

import overflowdb.Graph
import overflowdb.formats.Importer

import java.nio.file.Path
import scala.xml.{NodeSeq, XML}

/**
 * Imports GraphML into OverflowDB
 * Note: GraphML doesn't natively support list property types, so we fake it by encoding it as a `;` delimited string.
 *
 * https://en.wikipedia.org/wiki/GraphML
 * http://graphml.graphdrawing.org/primer/graphml-primer.html
 * */
object GraphMLImporter extends Importer {

  override def runImport(graph: Graph, inputFiles: Seq[Path]): Unit = {
    assert(inputFiles.size == 1, s"input must be exactly one file, but got ${inputFiles.size}")
    val doc = XML.loadFile(inputFiles.head.toFile)

    val keyEntries = doc \ "key"
    val graphXml = doc \ "graph"

    { // nodes
      val nodePropertyContextById = parsePropertyEntries("node", keyEntries)
      for (node <- graphXml \ "node") {
        addNode(graph, node, nodePropertyContextById)
      }
    }

    { // edges
      val edgePropertyContextById = parsePropertyEntries("edge", keyEntries)
      for (edge <- graphXml \ "edge") {
        addEdge(graph, edge, edgePropertyContextById)
      }
    }
  }

  private def parsePropertyEntries(forElementType: String, keyEntries: NodeSeq): Map[String, PropertyContext] = {
    keyEntries
      .filter(_ \@ "for" == forElementType)
      .map { node =>
        val id = node \@ "id"
        val name = node \@ "attr.name"
        val graphmlType = node \@ "attr.type"
        val tpe = Type.withName(graphmlType)
        (id, PropertyContext(name, tpe))
      }
      .toMap
  }

  private def addNode(graph: Graph, node: scala.xml.Node, propertyContextById: Map[String, PropertyContext]): Unit = {
    val id = node \@ "id"
    var label: Option[String] = None
    val keyValuePairs = Seq.newBuilder[String]

    for (entry <- node \ "data") {
      val value = entry.text
      entry \@ "key" match {
        case KeyForNodeLabel => label = Option(value)
        case propertyName => keyValuePairs.addAll(Seq(propertyName, value))
      }
    }

    for {
      id <- id.toLongOption
      label <- label
    } graph.addNode(id, label, keyValuePairs.result: _*)
  }

  private def addEdge(graph: Graph, edge: scala.xml.Node, propertyContextById: Map[String, PropertyContext]): Unit = {
    val sourceId = edge \@ "source"
    val targetId = edge \@ "target"
    var label: Option[String] = None
    val keyValuePairs = Seq.newBuilder[String]

    for (entry <- edge \ "data") {
      val value = entry.text
      entry \@ "key" match {
        case KeyForEdgeLabel => label = Option(value)
        case propertyName => keyValuePairs.addAll(Seq(propertyName, value))
      }
    }

    for {
      sourceId <- sourceId.toLongOption
      source <- Option(graph.node(sourceId))
      targetId <- targetId.toLongOption
      target <- Option(graph.node(targetId))
      label <- label
    } source.addEdge(label, target, keyValuePairs.result: _*)
  }
}
