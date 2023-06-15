package overflowdb.formats.graphson

import overflowdb.Graph
import overflowdb.formats.Importer
import overflowdb.formats.graphson.GraphSONProtocol._
import spray.json._

import java.nio.file.Path
import scala.io.Source.fromFile
import scala.util.Using

/** Imports OverflowDB graph to GraphSON 3.0
  *
  * https://tinkerpop.apache.org/docs/3.4.1/dev/io/#graphson-3d0
  */
object GraphSONImporter extends Importer {

  override def runImport(graph: Graph, inputFiles: Seq[Path]): Unit = {
    assert(inputFiles.size == 1, s"input must be exactly one file, but got ${inputFiles.size}")
    Using.resource(fromFile(inputFiles.head.toFile)) { source =>
      val graphSON = source.mkString.parseJson.convertTo[GraphSON]
      graphSON.`@value`.vertices.foreach(n => addNode(n, graph))
      graphSON.`@value`.edges.foreach(e => addEdge(e, graph))
    }
  }

  private def addNode(n: Vertex, graph: Graph): Unit = {
    graph.addNode(n.id.`@value`, n.label, flattenProperties(n.properties, graph): _*)
  }

  private def flattenProperties(m: Map[String, Property], graph: Graph): Array[_] = {
    m.view
      .mapValues { v =>
        v.`@value` match {
          case ListValue(value, _)   => value.map(_.`@value`)
          case NodeIdValue(value, _) => graph.node(value)
          case x                     => x.`@value`
        }
      }
      .flatMap { case (k, v) => Seq(k, v) }
      .toArray
  }

  private def addEdge(e: Edge, graph: Graph): Unit = {
    val src = graph.node(e.outV.`@value`)
    val tgt = graph.node(e.inV.`@value`)
    src.addEdge(e.label, tgt, flattenProperties(e.properties, graph): _*)
  }
}
