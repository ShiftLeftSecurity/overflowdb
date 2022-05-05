package overflowdb

package object formats {

  object Format extends Enumeration {
    val Neo4jCsv, GraphMl = Value

    lazy val byNameLowercase: Map[String, Format.Value] =
      values.map(format => (format.toString.toLowerCase, format)).toMap

    lazy val valuesAsStringLowercase: Seq[String] =
      byNameLowercase.values.toSeq.map(_.toString.toLowerCase).sorted

  }

}
