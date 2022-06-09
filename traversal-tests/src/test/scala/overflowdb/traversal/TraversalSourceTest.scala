package overflowdb.traversal

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import overflowdb._
import overflowdb.traversal.testdomains.simple.Thing.Properties.Name
import overflowdb.traversal.testdomains.simple.{Connection, SimpleDomain, Thing}

class TraversalSourceTest extends AnyWordSpec {

  "id starter step" when {

    "cast to given node type" in new Fixture {
      def thingViaId = traversal.id[Thing](one.id)
      thingViaId.out.next() shouldBe two1

      // `.outV` is only available on Traversal[Edge] (via EdgeTraversal)
      assertCompiles("thingViaId.out")
      assertDoesNotCompile("thingViaId.outV")
    }

    "default to `Node` if no type parameter given" in new Fixture {
      def thingViaId = traversal.id(one.id)
      thingViaId.out.next() shouldBe two1

      // `.outV` is only available on Traversal[Edge] (via EdgeTraversal)
      assertCompiles("thingViaId.out")
      assertDoesNotCompile("thingViaId.outV")
    }
  }

  "property lookup with and without indexes" in new Fixture {
    verifyTraversalResults()
    graph.indexManager.createNodePropertyIndex(Name.name)
    verifyTraversalResults()
  }

  private class Fixture {
    val graph = SimpleDomain.newGraph

    val one = graph + (Thing.Label, Name.of("one"))
    val two1 = graph + (Thing.Label, Name.of("two"))
    val two2 = graph + (Thing.Label, Name.of("two"))
    one.addEdge(Connection.Label, two1)

    def traversal = SimpleDomain.traversal(graph)

    def verifyTraversalResults() = {
      traversal.has(Name.of("one")).toSetMutable shouldBe Set(one)
      traversal.has(Name.of("two")).toSetMutable shouldBe Set(two1, two2)
      traversal.has(Name.of("unknown")).toSetMutable shouldBe Set.empty

      traversal.labelAndProperty(Thing.Label, Name.of("two")).toSetMutable shouldBe Set(two1, two2)
      traversal.labelAndProperty(Thing.Label, Name.of("unknown")).toSetMutable shouldBe Set.empty
      traversal.labelAndProperty("unknown", Name.of("two")).toSetMutable shouldBe Set.empty
    }

  }
}
