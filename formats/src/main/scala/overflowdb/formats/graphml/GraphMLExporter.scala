package overflowdb.formats.graphml

import overflowdb.{Element, Graph, Node}
import overflowdb.formats.{ExportResult, Exporter}

import java.nio.file.Path
import scala.collection.mutable
import scala.jdk.CollectionConverters.{IteratorHasAsScala, MapHasAsScala}

object GraphMLExporter extends Exporter {
  val KeyForNodeLabel = "labelV"
  val KeyForEdgeLabel = "labelE"

  /**
   * Exports OverflowDB Graph to graphml
   * https://en.wikipedia.org/wiki/GraphML
   * http://graphml.graphdrawing.org/primer/graphml-primer.html
   * */
  override def runExport(graph: Graph, outputRootDirectory: Path) = {
    val nodePropertyTypeByName = mutable.Map.empty[String, Type.Value]
    val edgePropertyTypeByName = mutable.Map.empty[String, Type.Value]

    val nodeEntries = graph.nodes().asScala.map { node =>
      s"""<node id="${node.id}">
         |    <data key="$KeyForNodeLabel">${node.label}</data>
         |    ${dataEntries("node", node, nodePropertyTypeByName)}
         |</node>
         |""".stripMargin
    }.toSeq

    val edgeEntries = graph.edges().asScala.map { edge =>
      s"""<edge source="${edge.inNode.id}" target="${edge.outNode.id}">
         |    <data key="$KeyForEdgeLabel">${edge.label}</data>
         |    ${dataEntries("edge", edge, edgePropertyTypeByName)}
         |</edge>
         |""".stripMargin
    }.toSeq

    val nodePropertyKeyEntries = nodePropertyTypeByName.map { case (key, tpe) =>
      s"""<key id="$key" for="node" attr.name="$key" attr.type="$tpe"></key>"""
    }.mkString("\n")
    val edgePropertyKeyEntries = edgePropertyTypeByName.map { case (key, tpe) =>
      s"""<key id="$key" for="edge" attr.name="$key" attr.type="$tpe"></key>"""
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
       |""".stripMargin

    if (!outputRootDirectory.toFile.exists)
//    val outFile = outputRootDirectory


    // TODO write to output dir/file

    ExportResult(
      nodeEntries.size,
      edgeEntries.size,
      files = Seq(outFile),
      None
    )
  }

  /** warning: updates type information based on runtime instances (in mutable.Map `propertyTypeByName`)
   */
  private def dataEntries(
      prefix: String,
      element: Element,
      propertyTypeByName: mutable.Map[String, Type.Value]): String = {
    element.propertiesMap.asScala.map { case (propertyName, propertyValue) =>
      // update type information based on runtime instances
      if (!propertyTypeByName.contains(propertyName)) {
        propertyTypeByName.update(propertyName, Type.fromRuntimeClass(propertyValue.getClass))
      }
      s"""<data key="${prefix}__${element.label}__$propertyName">$propertyValue</data>"""
    }.mkString("\n")
  }

  object Type extends Enumeration {
    val Boolean = Value("boolean")
    val Int = Value("int")
    val Long = Value("long")
    val Float = Value("float")
    val Double = Value("double")
    val String = Value("string")

    def fromRuntimeClass(clazz: Class[_]): Type.Value = {
      if (clazz.isAssignableFrom(classOf[Boolean]))
        Type.Boolean
      else if (clazz.isAssignableFrom(classOf[Int]))
        Type.Int
      else if (clazz.isAssignableFrom(classOf[Long]))
        Type.Long
      else if (clazz.isAssignableFrom(classOf[Float]))
        Type.Float
      else if (clazz.isAssignableFrom(classOf[Double]))
        Type.Double
      else if (clazz.isAssignableFrom(classOf[String]))
        Type.String
      else
        throw new AssertionError(s"unsupported runtime class `$clazz` - only ${Type.values.mkString("|")} are supported...}")
    }
  }
}
