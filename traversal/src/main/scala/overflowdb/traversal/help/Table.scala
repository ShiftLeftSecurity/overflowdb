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
      var maxRowWidth = idealRenderingWidth(columnNames)
      table.addRule()
      if (rows.nonEmpty) {
        rows.foreach { row =>
          table.addRow(row.asJava)
          maxRowWidth = math.max(maxRowWidth, idealRenderingWidth(row))
        }
      }
      table.addRule()
      table.getContext.setGridTheme(TA_GridThemes.FULL)
      table.setTextAlignment(TextAlignment.LEFT)
      val renderingWidth = math.min(maxRowWidth, availableWidthProvider.apply())
      table.render(renderingWidth)
    }
  }

  private def idealRenderingWidth(cells: Seq[String]): Int =
    cells.map(_.size).sum + cells.size + 1
}

object Table {
  trait AvailableWidthProvider extends (() => Int)

  class ConstantWidth(width: Int) extends AvailableWidthProvider {
    override def apply() = width
  }


}