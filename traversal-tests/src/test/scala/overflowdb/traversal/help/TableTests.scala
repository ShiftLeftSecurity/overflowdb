package overflowdb.traversal.help

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import overflowdb.traversal.help.Table.AvailableWidthProvider

class TableTests extends AnyWordSpec {

  "adjust table width based on widest row" when {

    "widest row is the header" in {
      implicit val availableWidthProvider: AvailableWidthProvider = new Table.ConstantWidth(50)
      val table = Table(
        Seq("column a", "column b"),
        Seq(
          Seq("abc 1", "bde 1"),
          Seq("abc 2", "bde 2")
        )
      )

      table.render.trim shouldBe
        """┌────────┬────────┐
          |│column a│column b│
          |├────────┼────────┤
          |│abc 1   │bde 1   │
          |│abc 2   │bde 2   │
          |└────────┴────────┘
          |""".stripMargin.trim
    }

    "widest row is the body" in {
      implicit val availableWidthProvider: AvailableWidthProvider = new Table.ConstantWidth(50)
      val table = Table(
        Seq("a", "b"),
        Seq(
          Seq("abc 1", "bde 1"),
          Seq("abc 2", "bde 2")
        )
      )

      table.render.trim shouldBe
        """┌─────┬─────┐
          |│a    │b    │
          |├─────┼─────┤
          |│abc 1│bde 1│
          |│abc 2│bde 2│
          |└─────┴─────┘
          |""".stripMargin.trim
    }

    "table is wider than the given available width" in {
      implicit val availableWidthProvider: AvailableWidthProvider = new Table.ConstantWidth(12)
      val table = Table(
        Seq("column a", "column b"),
        Seq(
          Seq("abc 1", "bde 1"),
          Seq("abc 2", "bde 2")
        )
      )

      table.render.trim shouldBe
        """┌─────┬────┐
          |│colum│colu│
          |│n a  │mn b│
          |├─────┼────┤
          |│abc 1│bde │
          |│     │1   │
          |│abc 2│bde │
          |│     │2   │
          |└─────┴────┘
          |""".stripMargin.trim
    }

  }

  "adapt to dynamically changing terminal width" in {
    val table = Table(
      Seq("lorem ipsum"),
      Seq(
        Seq("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et" +
          " dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip " +
          "ex ea commodo consequat."),
      )
    )

    var currentTerminalWidth = 80 // think "looking up current value from an actual terminal"
    implicit val availableWidthProvider: AvailableWidthProvider = () => currentTerminalWidth

    table.render.trim shouldBe
      """┌──────────────────────────────────────────────────────────────────────────────┐
        |│lorem ipsum                                                                   │
        |├──────────────────────────────────────────────────────────────────────────────┤
        |│Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor│
        |│incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis    │
        |│nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. │
        |└──────────────────────────────────────────────────────────────────────────────┘
        |""".stripMargin.trim

    currentTerminalWidth = 100 // emulating: terminal size has changed
    table.render.trim shouldBe
      """┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
        |│lorem ipsum                                                                                       │
        |├──────────────────────────────────────────────────────────────────────────────────────────────────┤
        |│Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut      │
        |│labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris │
        |│nisi ut aliquip ex ea commodo consequat.                                                          │
        |└──────────────────────────────────────────────────────────────────────────────────────────────────┘
        |""".stripMargin.trim
  }

}
