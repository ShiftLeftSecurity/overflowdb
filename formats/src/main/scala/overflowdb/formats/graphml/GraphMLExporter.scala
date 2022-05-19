package overflowdb.formats.graphml

import overflowdb.formats.{ExportResult, Exporter}
import overflowdb.{Element, Graph}

import java.nio.file.{Files, Path}
import scala.collection.mutable
import scala.jdk.CollectionConverters.{IterableHasAsScala, IteratorHasAsScala, MapHasAsScala}

/**
 * Exports OverflowDB Graph to GraphML
 * Note: GraphML doesn't natively support list property types, so we fake it by encoding it as a `;` delimited string.
 *
 * https://en.wikipedia.org/wiki/GraphML
 * http://graphml.graphdrawing.org/primer/graphml-primer.html
 * */
object GraphMLExporter extends Exporter {

  override def runExport(graph: Graph, outputRootDirectory: Path) = {
    val outFile = resolveOutputFile(outputRootDirectory)
    val nodePropertyContextById = mutable.Map.empty[String, PropertyContext]
    val edgePropertyContextById = mutable.Map.empty[String, PropertyContext]

    val nodeEntries = graph.nodes().asScala.map { node =>
      s"""<node id="${node.id}">
         |    <data key="$KeyForNodeLabel">${node.label}</data>
         |    ${dataEntries("node", node, nodePropertyContextById)}
         |</node>
         |""".stripMargin
    }.toSeq

    val edgeEntries = graph.edges().asScala.map { edge =>
      s"""<edge source="${edge.outNode.id}" target="${edge.inNode.id}">
         |    <data key="$KeyForEdgeLabel">${edge.label}</data>
         |    ${dataEntries("edge", edge, edgePropertyContextById)}
         |</edge>
         |""".stripMargin
    }.toSeq

    val nodePropertyKeyEntries = nodePropertyContextById.map { case (key, PropertyContext(name, tpe)) =>
      s"""<key id="$key" for="node" attr.name="$name" attr.type="$tpe"></key>"""
    }.mkString("\n")
    val edgePropertyKeyEntries = edgePropertyContextById.map { case (key, PropertyContext(name, tpe)) =>
      s"""<key id="$key" for="edge" attr.name="$name" attr.type="$tpe"></key>"""
    }.mkString("\n")

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
       |      ${nodeEntries.mkString("\n")}
       |      ${edgeEntries.mkString("\n")}
       |    </graph>
       |</graphml>
       |""".stripMargin.strip

    Files.writeString(outFile, xml)

    ExportResult(
      nodeEntries.size,
      edgeEntries.size,
      files = Seq(outFile),
      None
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
   */
  private def dataEntries(prefix: String,
                          element: Element,
                          propertyContextById: mutable.Map[String, PropertyContext]): String = {
    element.propertiesMap.asScala.map { case (propertyName, propertyValue) =>
      val encodedPropertyName = s"${prefix}__${element.label}__$propertyName"
      val graphMLTpe = Type.fromRuntimeClass(propertyValue.getClass)

      /* update type information based on runtime instances */
      def updatePropertyContext(graphMLTpe: => Option[Type.Value]) = {
        if (!propertyContextById.contains(encodedPropertyName)) {
          graphMLTpe.map { valueTpe =>
            propertyContextById.update(encodedPropertyName, PropertyContext(propertyName, valueTpe))
          }
        }
      }

      if (graphMLTpe == Type.List) {
        updatePropertyContext(tryDeriveListValueType(propertyValue))
        val valueEncoded = encodeListValue(propertyValue)
        s"""<data key="$encodedPropertyName">$valueEncoded</data>"""
      } else { // scalar value
        updatePropertyContext(Option(graphMLTpe))
        s"""<data key="$encodedPropertyName">$propertyValue</data>"""
      }
    }.mkString("\n")
  }

  private def tryDeriveListValueType(value: AnyRef): Option[Type.Value] = {
    val headOption =
      value match {
        case value: Iterable[_] =>
          value.headOption
        case value: IterableOnce[_] =>
          value.iterator.nextOption()
        case value: java.lang.Iterable[_] =>
          value.asScala.headOption
        case value: Array[_] =>
          value.headOption
        case _ => None
      }

    headOption.map(value => Type.fromRuntimeClass(value.getClass))
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
