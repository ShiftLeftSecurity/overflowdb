package overflowdb.formats.graphml

import overflowdb.Graph
import overflowdb.formats.Importer

import java.nio.file.Path
import scala.collection.immutable.ArraySeq
import scala.jdk.CollectionConverters.IterableHasAsJava
import scala.util.{Failure, Success, Try}
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

        // warning: this is derivating from the graphml spec - we do want to support list properties...
        val isList = graphmlType.endsWith("[]")
        val tpe =
          if (isList) Type.withName(graphmlType.dropRight(2))
          else Type.withName(graphmlType)
        (id, PropertyContext(name, tpe, isList))
      }
      .toMap
  }

  private def addNode(graph: Graph, node: scala.xml.Node, propertyContextById: Map[String, PropertyContext]): Unit = {
    val id = node \@ "id"
    var label: Option[String] = None
    val keyValuePairs = Seq.newBuilder[Any]

    for (entry <- node \ "data") {
      val value = entry.text
      entry \@ "key" match {
        case KeyForNodeLabel => label = Option(value)
        case key =>
          val PropertyContext(name, tpe, isList) = propertyContextById.get(key).getOrElse(
              throw new AssertionError(s"key $key not found in propertyContext..."))
          val convertedValue = convertValue(value, tpe, isList, context = node)
          keyValuePairs.addAll(Seq(name, convertedValue))
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
    val keyValuePairs = Seq.newBuilder[Any]

    for (entry <- edge \ "data") {
      val value = entry.text
      entry \@ "key" match {
        case KeyForEdgeLabel => label = Option(value)
        case key =>
          val PropertyContext(name, tpe, isList) = propertyContextById.get(key).getOrElse(
            throw new AssertionError(s"key $key not found in propertyContext..."))
          val convertedValue = convertValue(value, tpe, isList, context = edge)
          keyValuePairs.addAll(Seq(name, convertedValue))
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

  private def convertValue(stringValue: String, tpe: Type.Value, isList: Boolean, context: scala.xml.Node): Any = {
    if (isList) {
      val values = stringValue.split(';').map(value =>
        tryConvertScalarValue(value, tpe).get // if parsing fails, we do want to escalate
      )
      ArraySeq.unsafeWrapArray(values).asJava
    } else {
      tryConvertScalarValue(stringValue, tpe)
    } match {
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