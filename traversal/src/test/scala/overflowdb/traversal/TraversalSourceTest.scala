package overflowdb.traversal

import org.scalatest.{Matchers, WordSpec}
import overflowdb._
import overflowdb.traversal.testdomains.simple.{SimpleDomain, Thing}

class TraversalSourceTest extends WordSpec with Matchers {

  "property lookup with and without indexes" in {
    val graph = SimpleDomain.newGraph
    import Thing.Properties.Name

    val one = graph + (Thing.Label, Name -> "one")
    val two1 = graph + (Thing.Label, Name -> "two")
    val two2 = graph + (Thing.Label, Name -> "two")

    def verify() = {
      TraversalSource(graph).has(Name -> "one").toSet shouldBe Set(one)
      TraversalSource(graph).has(Name -> "two").toSet shouldBe Set(two1, two2)
      TraversalSource(graph).has(Name -> "unknown").toSet shouldBe Set.empty

      TraversalSource(graph).labelAndProperty(Thing.Label, Name -> "two").toSet shouldBe Set(two1, two2)
      TraversalSource(graph).labelAndProperty(Thing.Label, Name -> "unknown").toSet shouldBe Set.empty
      TraversalSource(graph).labelAndProperty("unknown", Name -> "two").toSet shouldBe Set.empty
    }

    verify()
    graph.indexManager.createNodePropertyIndex(Name.name)
    verify()
  }

}
