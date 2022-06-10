package overflowdb.formats.graphml

import overflowdb.BatchedUpdate.{DiffGraph, DiffGraphBuilder}
import overflowdb.{BatchedUpdate, DetachedNodeData, Graph}
import overflowdb.formats.Importer

import java.nio.file.Path
import scala.util.{Failure, Success, Try}
import scala.xml.{NodeSeq, XML}

/**
 * Imports GraphML into OverflowDB (via a DiffGraph)
 *
 * https://en.wikipedia.org/wiki/GraphML
 * http://graphml.graphdrawing.org/primer/graphml-primer.html
 * */
object GraphMLImporter extends Importer {

  override def createDiffGraph(inputFiles: Seq[Path]): DiffGraph = {
    assert(inputFiles.size == 1, s"input must be exactly one file, but got ${inputFiles.size}")
    val doc = XML.loadFile(inputFiles.head.toFile)
    val diffGraph = new DiffGraphBuilder

    val keyEntries = doc \ "key"
    val graphXml = doc \ "graph"

    val nodePropertyContextById = parsePropertyEntries("node", keyEntries)
    val nodesById = (graphXml \ "node").map { node =>
      addNode(diffGraph, node, nodePropertyContextById)
    }.toMap

    val edgePropertyContextById = parsePropertyEntries("edge", keyEntries)
    for (edge <- graphXml \ "edge") {
      addEdge(diffGraph, edge, edgePropertyContextById, nodesById)
    }

    diffGraph.build()
  }

  private def parsePropertyEntries(forElementType: String, keyEntries: NodeSeq): Map[String, PropertyContext] = {
    keyEntries
      .filter(_ \@ "for" == forElementType)
      .map { node =>
        val id = node \@ "id"
        val name = node \@ "attr.name"
        val graphmlType = node \@ "attr.type"
        (id, PropertyContext(name, Type.withName(graphmlType)))
      }
      .toMap
  }

  private def addNode(diffGraph: DiffGraphBuilder, node: scala.xml.Node, propertyContextById: Map[String, PropertyContext]): (Long, DetachedNodeData) = {
    val id = node \@ "id"
    val idLong = id.toLongOption.getOrElse(throw new AssertionError(s"id expected but not set on $node"))
    var label: Option[String] = None
    val keyValuePairs = Seq.newBuilder[Any]

    for (entry <- node \ "data") {
      val value = entry.text
      entry \@ "key" match {
        case KeyForNodeLabel => label = Option(value)
        case key =>
          val PropertyContext(name, tpe) = propertyContextById.get(key).getOrElse(
            throw new AssertionError(s"key $key not found in propertyContext..."))
          val convertedValue = convertValue(value, tpe, context = node)
          keyValuePairs.addAll(Seq(name, convertedValue))
      }
    }

    assert(label.isDefined, s"label not found in $node")
    val newNode = diffGraph.addAndReturnNode(label.get, keyValuePairs.result: _*)
    newNode.setRefOrId(idLong)
    (idLong, newNode)
  }

  private def addEdge(diffGraph: DiffGraphBuilder, edge: scala.xml.Node, propertyContextById: Map[String, PropertyContext], nodesById: Map[Long, DetachedNodeData]): Unit = {
    val sourceId = edge \@ "source"
    val targetId = edge \@ "target"
    var label: Option[String] = None
    val keyValuePairs = Seq.newBuilder[Any]

    for (entry <- edge \ "data") {
      val value = entry.text
      entry \@ "key" match {
        case KeyForEdgeLabel => label = Option(value)
        case key =>
          val PropertyContext(name, tpe) = propertyContextById.get(key).getOrElse(
            throw new AssertionError(s"key $key not found in propertyContext..."))
          val convertedValue = convertValue(value, tpe, context = edge)
          keyValuePairs.addAll(Seq(name, convertedValue))
      }
    }
    assert(label.isDefined, s"label not found in $edge")

    for {
      sourceId <- sourceId.toLongOption
      targetId <- targetId.toLongOption
    } {
      Seq(sourceId, targetId).foreach { id =>
        assert(nodesById.contains(id), s"node with id=$sourceId not found in `nodesById` - available ids are: ${nodesById.keys.toSeq.sorted}")
      }
      val source = nodesById(sourceId)
      val destination = nodesById(targetId)
      diffGraph.addEdge(source, destination, label.get, keyValuePairs.result: _*)
    }
  }

  private def convertValue(stringValue: String, tpe: Type.Value, context: scala.xml.Node): Any = {
    tryConvertScalarValue(stringValue, tpe) match {
      case Success(value) => value
      case Failure(e) => throw new AssertionError(
        s"unable to parse `$stringValue` of tpe=$tpe. context: $context", e)
    }
  }

  private def tryConvertScalarValue(stringValue: String, tpe: Type.Value): Try[Any] = {
    Try {
      tpe match {
        case Type.Boolean => stringValue.toBoolean
        case Type.Int => stringValue.toInt
        case Type.Long => stringValue.toLong
        case Type.Float => stringValue.toLong
        case Type.Double => stringValue.toDouble
        case Type.String => stringValue
      }
    }
  }
}
