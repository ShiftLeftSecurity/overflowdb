package overflowdb.traversal.testdomains.simple

import overflowdb._
import overflowdb.traversal.Traversal

/* simple example graph:
 * L3 <- L2 <- L1 <- Center -> R1 -> R2 -> R3 -> R4
 * */
object ExampleGraphSetup {
  val nonExistingLabel = "this label does not exist"
  val nonExistingPropertyKey = new PropertyKey[String]("this property key does not exist")
  val graph = SimpleDomain.newGraph

  val l3 = addThing("L3")
  val l2 = addThing("L2")
  val l1 = addThing("L1")
  val center = addThing("Center")
  val r1 = addThing("R1")
  val r2 = addThing("R2")
  val r3 = addThing("R3")
  val r4 = addThing("R4")
  val r5 = addThing("R5")

  center --- Connection.Label --> l1
  l1 --- Connection.Label --> l2
  l2 --- Connection.Label --> l3
  center --- Connection.Label --> r1
  r1 --- (Connection.Label, Connection.Properties.Distance.of(10)) --> r2
  r2 --- (Connection.Label, Connection.Properties.Distance.of(10)) --> r3
  r3 --- (Connection.Label, Connection.Properties.Distance.of(13)) --> r4
  r4 --- (Connection.Label, Connection.Properties.Distance.of(14)) --> r5

  def simpleDomain: SimpleDomainTraversalSource = SimpleDomain.traversal(graph)
  def centerTrav = Traversal.fromSingle(center)

  private def addThing(name: String): Thing = {
    val node = graph + (Thing.Label, Thing.Properties.Name.of(name))
    node.asInstanceOf[Thing]
  }

}
