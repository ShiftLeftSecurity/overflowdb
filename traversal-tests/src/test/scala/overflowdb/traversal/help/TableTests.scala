package overflowdb.traversal.help

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import overflowdb.traversal.help.Table.AvailableWidthProvider

class TableTests extends AnyWordSpec {

  "render a nice generic table" in {
    val table = Table(
      Seq("column a", "column b"),
      Seq(
        Seq("abc 1", "bde 1"),
        Seq("abc 2", "bde 2 - little more content here")
      )
    )

    implicit val availableWidthProvider: AvailableWidthProvider = new Table.ConstantWidth(50)
    table.render.trim shouldBe
      """┌────────┬────────────────────────────────┐
        |│column a│column b                        │
        |├────────┼────────────────────────────────┤
        |│abc 1   │bde 1                           │
        |│abc 2   │bde 2 - little more content here│
        |└────────┴────────────────────────────────┘
        |""".stripMargin.trim
  }

  "adapt to dynamically changing terminal width" in {
    val table = Table(
      Seq("lorem ipsum"),
      Seq(
        Seq(
          "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et" +
            " dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip"
        )
      )
    )

    var currentTerminalWidth = 50 // think "looking up current value from an actual terminal"
    implicit val availableWidthProvider: AvailableWidthProvider = () => currentTerminalWidth

    table.render.trim shouldBe
      """┌───────────────────────────────────────────────────────┐
        |│lorem ipsum                                            │
        |├───────────────────────────────────────────────────────┤
        |│Lorem ipsum dolor sit amet, consectetur adipiscing     │
        |│elit, sed do eiusmod tempor incididunt ut labore et    │
        |│dolore magna aliqua. Ut enim ad minim veniam, quis     │
        |│nostrud exercitation ullamco laboris nisi ut aliquip   │
        |└───────────────────────────────────────────────────────┘
        |""".stripMargin.trim

    currentTerminalWidth = 100 // emulating: terminal size has changed
    table.render.trim shouldBe
      """┌───────────────────────────────────────────────────────────────────────────────────────────────┐
        |│lorem ipsum                                                                                    │
        |├───────────────────────────────────────────────────────────────────────────────────────────────┤
        |│Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut   │
        |│labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco      │
        |│laboris nisi ut aliquip                                                                        │
        |└───────────────────────────────────────────────────────────────────────────────────────────────┘
        |""".stripMargin.trim
  }

}
