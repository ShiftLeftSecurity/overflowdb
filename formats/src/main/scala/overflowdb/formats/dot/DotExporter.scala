package overflowdb.formats.dot

import overflowdb.formats.{ExportResult, Exporter, iterableForList}
import overflowdb.{Edge, Graph, Node}

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.{IterableHasAsScala, MapHasAsScala}
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

  override def runExport(graph: Graph, outputRootDirectory: Path) = {
    val outFile = resolveOutputFile(outputRootDirectory)
    var nodeCount, edgeCount = 0

    Using.resource(Files.newBufferedWriter(outFile)) { writer =>
      def writeLine(line: String): Unit = {
        writer.write(line)
        writer.newLine()
      }

      writeLine("digraph {")

      graph.nodes().forEachRemaining { node =>
        nodeCount += 1
        writeLine(node2Dot(node))
      }

      graph.edges().forEachRemaining { edge =>
        edgeCount += 1
        writeLine(edge2Dot(edge))
      }

      writeLine("}")
      writer.flush()
      writer.close()
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
      case value: String => s"\"$value\""
      case list if iterableForList.isDefinedAt(list) =>
        val values = iterableForList(list).mkString(";")
        s"\"$values\""
      case value => value.toString
    }
  }

  private def resolveOutputFile(outputRootDirectory: Path): Path = {
    if (Files.exists(outputRootDirectory)) {
      assert(Files.isDirectory(outputRootDirectory), s"given output directory `$outputRootDirectory` must be a directory, but isn't...")
    } else {
      Files.createDirectories(outputRootDirectory)
    }
    outputRootDirectory.resolve("export.dot")
  }

  private def encodeListValue(value: AnyRef): String = {
    value match {
      case value: Iterable[_] =>
        value.mkString(";")
      case value: IterableOnce[_] =>
        value.iterator.mkString(";")
      case value: java.lang.Iterable[_] =>
        value.asScala.mkString(";")
      case value: Array[_] =>
        value.mkString(";")
      case _ => value.toString
    }
  }

}