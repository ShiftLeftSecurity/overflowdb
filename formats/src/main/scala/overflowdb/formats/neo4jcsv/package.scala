package overflowdb.formats

package object neo4jcsv {

  private[neo4jcsv] object FileType extends Enumeration {
    val Nodes = Value
    val Relationships = Value
  }

}
