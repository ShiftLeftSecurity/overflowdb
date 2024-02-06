package overflowdb.traversal.help

import de.vandermeer.asciitable.AsciiTable
import de.vandermeer.skb.interfaces.transformers.textformat.TextAlignment

import scala.jdk.CollectionConverters.SeqHasAsJava

case class Table(columnNames: Seq[String], rows: Seq[Seq[String]]) {

  def render(width: Int = 120): String = {
    if (columnNames.isEmpty && rows.isEmpty) {
      ""
    } else {
      val table = new AsciiTable()
      table.addRule()
      table.addRow(columnNames.asJava)
      table.addRule()
      if (rows.nonEmpty) {
        rows.foreach { row =>
          table.addRow(row.asJava)
          table.addRule()
        }
      }
      table.setTextAlignment(TextAlignment.LEFT)
      table.render(width)
    }
  }

}