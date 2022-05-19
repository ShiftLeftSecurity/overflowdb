package overflowdb

import scala.jdk.CollectionConverters.MapHasAsScala

package object formats {
  object Format extends Enumeration {
    val Neo4jCsv, GraphMl = Value

    lazy val byNameLowercase: Map[String, Format.Value] =
      values.map(format => (format.toString.toLowerCase, format)).toMap

    lazy val valuesAsStringLowercase: Seq[String] =
      byNameLowercase.values.toSeq.map(_.toString.toLowerCase).sorted
  }

  private[formats] def labelsWithNodes(graph: Graph): Seq[String] = {
    graph.nodeCountByLabel.asScala.collect {
      case (label, count) if count > 0 => label
    }.toSeq
  }

  /**
   * @return true if the given class is either array or a (subclass of) Java Iterable or Scala IterableOnce
   */
  def isList(clazz: Class[_]): Boolean = {
    clazz.isArray ||
      classOf[java.lang.Iterable[_]].isAssignableFrom(clazz) ||
      classOf[IterableOnce[_]].isAssignableFrom(clazz)
  }
}
