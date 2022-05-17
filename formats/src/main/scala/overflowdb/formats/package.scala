package overflowdb

import java.nio.file.Path
import scala.jdk.CollectionConverters.MapHasAsScala

package object formats {
  object Format extends Enumeration {
    val Neo4jCsv, GraphMl = Value

    lazy val byNameLowercase: Map[String, Format.Value] =
      values.map(format => (format.toString.toLowerCase, format)).toMap

    lazy val valuesAsStringLowercase: Seq[String] =
      byNameLowercase.values.toSeq.map(_.toString.toLowerCase).sorted
  }

  private [formats] case class CountAndFiles(count: Int, files: Seq[Path]) {
    def plus(other: CountAndFiles): CountAndFiles =
      CountAndFiles(count + other.count, files ++ other.files)
  }

  def labelsWithNodes(graph: Graph): Seq[String] = {
    graph.nodeCountByLabel.asScala.collect {
      case (label, count) if count > 0 => label
    }.toSeq
  }
}
