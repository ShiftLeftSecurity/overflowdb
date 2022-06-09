package overflowdb.formats.graphml

import overflowdb.formats.{ExportResult, Exporter, isList, writeFile}
import overflowdb.{Element, Graph}

import java.lang.System.lineSeparator
import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.jdk.CollectionConverters.{IteratorHasAsScala, MapHasAsScala}
import scala.xml.{PrettyPrinter, XML}

/**
 * Exports OverflowDB Graph to GraphML
 *
 * Warning: list properties are not natively supported by graphml...
 * We initially built some support for those which deviated from the spec, but given that other tools don't support
 * it, some refusing to import the remainder, we've dropped it. Now, lists are serialised to `;`-separated strings.
 *
 * https://en.wikipedia.org/wiki/GraphML
 * http://graphml.graphdrawing.org/primer/graphml-primer.html
 * */
object GraphMLExporter extends Exporter {

  override def runExport(graph: Graph, outputRootDirectory: Path) = {
    val outFile = resolveOutputFile(outputRootDirectory)
    val nodePropertyContextById = mutable.Map.empty[String, PropertyContext]
    val edgePropertyContextById = mutable.Map.empty[String, PropertyContext]
    val discardedListPropertyCount = new AtomicInteger(0)

    val nodeEntries = graph.nodes().asScala.map { node =>
      s"""<node id="${node.id}">
         |    <data key="$KeyForNodeLabel">${node.label}</data>
         |    ${dataEntries("node", node, nodePropertyContextById, discardedListPropertyCount)}
         |</node>
         |""".stripMargin
    }.toSeq

    val edgeEntries = graph.edges().asScala.map { edge =>
      s"""<edge source="${edge.outNode.id}" target="${edge.inNode.id}">
         |    <data key="$KeyForEdgeLabel">${edge.label}</data>
         |    ${dataEntries("edge", edge, edgePropertyContextById, discardedListPropertyCount)}
         |</edge>
         |""".stripMargin
    }.toSeq

    def propertyKeyXml(forAttr: String, propsMap: mutable.Map[String, PropertyContext]): String = {
      propsMap.map { case (key, PropertyContext(name, tpe)) =>
        s"""<key id="$key" for="$forAttr" attr.name="$name" attr.type="$tpe"></key>"""
      }.mkString(lineSeparator)
    }
    val nodePropertyKeyEntries = propertyKeyXml("node", nodePropertyContextById)
    val edgePropertyKeyEntries = propertyKeyXml("edge", edgePropertyContextById)

    val xml = s"""
       |<?xml version="1.0" encoding="UTF-8"?>
       |<graphml xmlns="http://graphml.graphdrawing.org/xmlns"
       |  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       |  xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd">
       |    <key id="$KeyForNodeLabel" for="node" attr.name="$KeyForNodeLabel" attr.type="string"></key>
       |    <key id="$KeyForEdgeLabel" for="edge" attr.name="$KeyForEdgeLabel" attr.type="string"></key>
       |    $nodePropertyKeyEntries
       |    $edgePropertyKeyEntries
       |    <graph id="G" edgedefault="directed">
       |      ${nodeEntries.mkString(lineSeparator)}
       |      ${edgeEntries.mkString(lineSeparator)}
       |    </graph>
       |</graphml>
       |""".stripMargin.trim
    writeFile(outFile, xml)
    xmlFormatInPlace(outFile)

    val additionalInfo =
      Some(discardedListPropertyCount.get)
        .filter(_ > 0)
        .map { count =>
          s"warning: discarded $count list properties (because they are not supported by the graphml spec)"
        }

    ExportResult(
      nodeCount = nodeEntries.size,
      edgeCount = edgeEntries.size,
      files = Seq(outFile),
      additionalInfo
    )
  }

  private def resolveOutputFile(outputRootDirectory: Path): Path = {
    if (Files.exists(outputRootDirectory)) {
      assert(Files.isDirectory(outputRootDirectory), s"given output directory `$outputRootDirectory` must be a directory, but isn't...")
    } else {
      Files.createDirectories(outputRootDirectory)
    }
    outputRootDirectory.resolve("export.graphml")
  }

  /**
   * warning: updates type information based on runtime instances (in mutable.Map `propertyTypeByName`)
   * warning2: updated the `discardedListPropertyCount` counter - if we need to discard any list properties, display a warning to the user
   */
  private def dataEntries(prefix: String,
                          element: Element,
                          propertyContextById: mutable.Map[String, PropertyContext],
                          discardedListPropertyCount: AtomicInteger): String = {
    element.propertiesMap.asScala.map { case (propertyName, propertyValue) =>
      if (isList(propertyValue.getClass)) {
        discardedListPropertyCount.incrementAndGet()
        "" // discard list properties
      } else { // scalar value
        val encodedPropertyName = s"${prefix}__${element.label}__$propertyName"
        val graphMLTpe = Type.fromRuntimeClass(propertyValue.getClass)

        /* update type information based on runtime instances */
          if (!propertyContextById.contains(encodedPropertyName)) {
            propertyContextById.update(encodedPropertyName, PropertyContext(propertyName, graphMLTpe))
          }
        val xmlEncoded = xml.Utility.escape(propertyValue.toString)
        s"""<data key="$encodedPropertyName">$xmlEncoded</data>"""
      }
    }.mkString(lineSeparator)
  }

  private def xmlFormatInPlace(xmlFile: Path): Unit = {
    val xml = XML.loadFile(xmlFile.toFile)
    val prettyPrinter = new PrettyPrinter(120, 2)
    val formatted = prettyPrinter.format(xml)
    writeFile(xmlFile, formatted)
  }

}