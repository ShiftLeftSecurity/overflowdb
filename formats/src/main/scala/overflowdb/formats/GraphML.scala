package overflowdb.formats

import java.nio.file.{Path, Paths}
import overflowdb.Graph

import scala.xml.XML

/** primitive GraphML importer which doesn't support much from the spec...
 *  only enough to get us covered for some standard test cases, really */
object GraphML {
  def insert(graphMLFile: String, graph: Graph): Unit =
    insert(Paths.get(graphMLFile), graph)

  def insert(graphMLFile: Path, graph: Graph): Unit = {
    val doc = XML.loadFile(graphMLFile.toFile)
    val graphXml = doc \ "graph"
    for (node <- graphXml \ "node") {
      addNode(graph, node)
    }
    for (edge <- graphXml \ "edge") {
      addEdge(graph, edge)
    }
  }

  private def addNode(graph: Graph, node: scala.xml.Node): Unit = {
    val id = node \@ "id"
    var label: Option[String] = None
    val keyValuePairs = Seq.newBuilder[String]

    for (entry <- node \ "data") {
      val value = entry.text
      entry \@ "key" match {
        case "labelV" => label = Option(value)
        case propertyName => keyValuePairs.addAll(Seq(propertyName, value))
      }
    }

    for {
      id <- id.toLongOption
      label <- label
    } graph.addNode(id, label, keyValuePairs.result: _*)
  }

  private def addEdge(graph: Graph, edge: scala.xml.Node): Unit = {
    val sourceId = edge \@ "source"
    val targetId = edge \@ "target"
    var label: Option[String] = None
    val keyValuePairs = Seq.newBuilder[String]

    for (entry <- edge \ "data") {
      val value = entry.text
      entry \@ "key" match {
        case "labelE" => label = Option(value)
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
