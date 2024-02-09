package overflowdb.traversal.help

import de.vandermeer.asciitable.AsciiTable
import de.vandermeer.asciithemes.TA_GridThemes
import de.vandermeer.skb.interfaces.transformers.textformat.TextAlignment
import overflowdb.traversal.help.Table._

import scala.jdk.CollectionConverters.SeqHasAsJava

case class Table(columnNames: Seq[String], rows: Seq[Seq[String]]) {

  def render(implicit availableWidthProvider: AvailableWidthProvider): String = {
    if (columnNames.isEmpty && rows.isEmpty) {
      ""
    } else {
      val table = new AsciiTable()
      table.addRule()
      table.addRow(columnNames.asJava)
      table.addRule()
      if (rows.nonEmpty) {
        rows.map(_.asJava).foreach(table.addRow)
      }
      table.addRule()
      table.getContext.setGridTheme(TA_GridThemes.FULL)
      table.setTextAlignment(TextAlignment.LEFT)

      // some terminal emulators (e.g. on github actions CI) report to have a width of 0...
      // that doesn't work for rendering a table, so we compensate by using a minimum width
      val renderingWidth = math.max(availableWidthProvider.apply(), 60)
      table.render(renderingWidth)
    }
  }

}

object Table {
  trait AvailableWidthProvider extends (() => Int)

  class ConstantWidth(width: Int) extends AvailableWidthProvider {
    override def apply() = width
  }

}
