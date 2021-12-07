package overflowdb.traversal

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import overflowdb._
import overflowdb.traversal.testdomains.simple.{SimpleDomain, Thing}

class TraversalSourceTest extends AnyWordSpec {

  "property lookup with and without indexes" in {
    val graph = SimpleDomain.newGraph
    import Thing.Properties.Name

    val one = graph + (Thing.Label, Name.of("one"))
    val two1 = graph + (Thing.Label, Name.of("two"))
    val two2 = graph + (Thing.Label, Name.of("two"))

    def verify() = {
      TraversalSource(graph).has(Name.of("one")).toSetMutable shouldBe Set(one)
      TraversalSource(graph).has(Name.of("two")).toSetMutable shouldBe Set(two1, two2)
      TraversalSource(graph).has(Name.of("unknown")).toSetMutable shouldBe Set.empty

      TraversalSource(graph).labelAndProperty(Thing.Label, Name.of("two")).toSetMutable shouldBe Set(two1, two2)
      TraversalSource(graph).labelAndProperty(Thing.Label, Name.of("unknown")).toSetMutable shouldBe Set.empty
      TraversalSource(graph).labelAndProperty("unknown", Name.of("two")).toSetMutable shouldBe Set.empty
    }

    verify()
    graph.indexManager.createNodePropertyIndex(Name.name)
    verify()
  }

}
