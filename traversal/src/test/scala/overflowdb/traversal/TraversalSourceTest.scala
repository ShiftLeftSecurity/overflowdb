package overflowdb.traversal

import org.scalatest.{Matchers, WordSpec}
import overflowdb._
import overflowdb.traversal.testdomains.simple.{SimpleDomain, Thing}

class TraversalSourceTest extends WordSpec with Matchers {

  "property lookup with and without indexes" in {
    val graph = SimpleDomain.newGraph
    import Thing.Properties.Name

    val one = graph + (Thing.Label, Name.of("one"))
    val two1 = graph + (Thing.Label, Name.of("two"))
    val two2 = graph + (Thing.Label, Name.of("two"))

    def verify() = {
      TraversalSource(graph).has(Name.of("one")).toSet shouldBe Set(one)
      TraversalSource(graph).has(Name.of("two")).toSet shouldBe Set(two1, two2)
      TraversalSource(graph).has(Name.of("unknown")).toSet shouldBe Set.empty

      TraversalSource(graph).labelAndProperty(Thing.Label, Name.of("two")).toSet shouldBe Set(two1, two2)
      TraversalSource(graph).labelAndProperty(Thing.Label, Name.of("unknown")).toSet shouldBe Set.empty
      TraversalSource(graph).labelAndProperty("unknown", Name.of("two")).toSet shouldBe Set.empty
    }

    verify()
    graph.indexManager.createNodePropertyIndex(Name.name)
    verify()
  }

}
