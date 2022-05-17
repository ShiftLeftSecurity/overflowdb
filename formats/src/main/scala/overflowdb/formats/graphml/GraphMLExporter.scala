package overflowdb.formats.graphml

import overflowdb.Graph
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
    val propertyTypeByName = mutable.Map.empty[String, Type.Value]

    val nodes = graph.nodes().asScala.map { node =>
      val keyEntries = {
        node.propertiesMap().asScala.map { case (key, value) =>
          // update type information based on runtime instances
          if (!propertyTypeByName.contains(key)) {
            propertyTypeByName.update(key, Type.fromRuntimeClass(value.getClass))
          }
          s"""<data key="$key">$value</data>"""
        }
      }.mkString("\n")

      s"""<node id="${node.id}">
         |    <data key="$KeyForNodeLabel">${node.label}</data>
         |    $keyEntries
         |</node>
         |""".stripMargin
    }

    // TODO propertyTypeByName needs to be grouped by label... properties could have different types for different labels...

    //    graph.edges().forEachRemaining { edge =>
//
//    }

    // TODO write to output dir/file
        ExportResult(
          nodes.size,
          -1,
          files = Nil,
          None
        )
  }

  private def xmlDocument(body: String) =
    s"""
      |<?xml version="1.0" encoding="UTF-8"?>
      |<graphml xmlns="http://graphml.graphdrawing.org/xmlns"
      |    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      |    xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns
      |     http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd">
      |$body
      |</graphml>
      |""".stripMargin

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
