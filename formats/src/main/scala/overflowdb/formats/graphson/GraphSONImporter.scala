package overflowdb.formats.graphson

import overflowdb.Graph
import overflowdb.formats.Importer
import overflowdb.formats.graphson.GraphSONProtocol._
import spray.json._

import java.nio.file.Path
import scala.io.Source.fromFile

object GraphSONImporter extends Importer {
  override def runImport(graph: Graph, inputFiles: Seq[Path]): Unit = {
    assert(inputFiles.size == 1, s"input must be exactly one file, but got ${inputFiles.size}")
    val source = fromFile(inputFiles.head.toFile)
    val graphSON = try source.mkString.parseJson.convertTo[GraphSON]
    finally source.close()
    graphSON.`@value`.vertices.foreach(n => addNode(n, graph))
    graphSON.`@value`.edges.foreach(e => addEdge(e, graph))
  }

  private def addNode(n: Vertex, graph: Graph): Unit = {
    graph.addNode(n.id.`@value`, n.label, unboxVertexProperties(n.properties): _*)
  }

  private def unboxVertexProperties(m: Map[String, VertexProperty]): Array[_] = {
    m.flatMap { case (k, v) => Seq(k, v.`@value`) }.toArray
  }

  private def unboxEdgeProperties(m: Map[String, Property]): Array[_] = {
    m.flatMap { case (k, v) => Seq(k, v.`@value`) }.toArray
  }

  private def addEdge(e: Edge, graph: Graph): Unit = {
    val src = graph.node(e.inV.`@value`)
    val tgt = graph.node(e.outV.`@value`)
    src.addEdge(e.label, tgt, unboxEdgeProperties(e.properties))
  }
}
