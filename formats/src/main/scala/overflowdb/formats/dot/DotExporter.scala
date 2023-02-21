package overflowdb.formats.dot

import org.apache.commons.text.StringEscapeUtils
import overflowdb.formats.{ExportResult, Exporter, iterableForList, resolveOutputFileSingle}
import overflowdb.{Edge, Node}

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.util.Using

/**
 * Exports OverflowDB Graph to graphviz dot/gv file
 *
 * Note: GraphML doesn't natively support list property types, so we fake it by encoding it as a `;` delimited string.
 * If you import this into a different database, you'll need to parse that separately.
 *
 * https://en.wikipedia.org/wiki/DOT_(graph_description_language)
 * https://www.graphviz.org/doc/info/lang.html
 * http://magjac.com/graphviz-visual-editor/
 * https://www.slideshare.net/albazo/graphiz-using-the-dot-language
 * */
object DotExporter extends Exporter {
  override def defaultFileExtension = "dot"

  override def runExport(nodes: IterableOnce[Node], edges: IterableOnce[Edge], outputFile: Path) = {
    val outFile = resolveOutputFileSingle(outputFile, s"export.$defaultFileExtension")
    var nodeCount, edgeCount = 0

    Using.resource(Files.newBufferedWriter(outFile)) { writer =>
      def writeLine(line: String): Unit = {
        writer.write(line)
        writer.newLine()
      }

      writeLine("digraph {")

      nodes.iterator.foreach { node =>
        nodeCount += 1
        writeLine(node2Dot(node))
      }

      edges.iterator.foreach { edge =>
        edgeCount += 1
        writeLine(edge2Dot(edge))
      }

      writeLine("}")
    }

    ExportResult(
      nodeCount = nodeCount,
      edgeCount = edgeCount,
      files = Seq(outFile),
      additionalInfo = None
    )
  }

  private def node2Dot(node: Node): String = {
    s"  ${node.id} [label=${node.label} ${properties2Dot(node.propertiesMap)}]"
  }

  private def edge2Dot(edge: Edge): String = {
    s"  ${edge.outNode.id} -> ${edge.inNode.id} [label=${edge.label} ${properties2Dot(edge.propertiesMap)}]"
  }

  private def properties2Dot(properties: java.util.Map[String, Object]): String = {
    properties.asScala.map { case (key, value) =>
      s"$key=${encodePropertyValue(value)}"
    }.mkString(" ")
  }

  private def encodePropertyValue(value: Object): String = {
    value match {
      case value: String =>
        val escaped = value
          .replace("""\""", """\\""") // escape escape chars - this should come first
          .replace("\"", "\\\"") // escape double quotes, because we use them to enclose strings
        s"\"$escaped\""
      case list if iterableForList.isDefinedAt(list) =>
        val values = iterableForList(list).mkString(";")
        s"\"$values\""
      case value => value.toString
    }
  }
}
