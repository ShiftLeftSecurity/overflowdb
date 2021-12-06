package overflowdb

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import overflowdb.traversal._
import overflowdb.traversal.testdomains.simple.Connection.Properties.Distance
import overflowdb.traversal.testdomains.simple.{Connection, SimpleDomain, Thing}
import overflowdb.traversal.testdomains.simple.Thing.Properties._

class GraphSugarTests extends AnyWordSpec {

  "graph + label" can {
    "add a node" in {
      val graph = SimpleDomain.newGraph
      graph + Thing.Label
      graph.nodeCount shouldBe 1
    }

    "add a node with given id" in {
      val graph = SimpleDomain.newGraph
      graph + (Thing.Label, 99)
      graph.nodeOption(99).isDefined shouldBe true
    }

    "add a node with property" in {
      val graph = SimpleDomain.newGraph
      graph + (Thing.Label, Name.of("one thing"))
      graph.nodeCount shouldBe 1
      SimpleDomain.traversal(graph).things.name.toList shouldBe List("one thing")
    }

    "add nodes with multiple properties" in {
      val graph = SimpleDomain.newGraph
      graph + (Thing.Label, Name -> "one thing")
      graph + (Thing.Label, Name -> "another thing", Size -> 42)
      SimpleDomain.traversal(graph).things.propertiesMap.toSetMutable shouldBe Set(
        Map(("name", "one thing")),
        Map(("name", "another thing"), ("size", 42))
      )
    }

    "add a node with property and id" in {
      val graph = SimpleDomain.newGraph
      graph + (Thing.Label, 99, Name.of("one thing"))
      SimpleDomain.traversal(graph).things.name.toList shouldBe List("one thing")
      graph.node(99).property("name") shouldBe "one thing"
    }

    "fail for unknown nodeType" in {
      val graph = SimpleDomain.newGraph
      intercept[IllegalArgumentException] {
        graph + "unknown"
      }
      graph.nodeCount shouldBe 0
    }
  }

  "nodeOption" can {
    "retrieve a node, or not" in {
      val graph = SimpleDomain.newGraph
      val node = graph + Thing.Label
      graph.nodeOption(node.id) shouldBe Some(node)
      graph.nodeOption(node.id + 1) shouldBe None
    }
  }

  "arrow syntax" can {
    "add an edge" in {
      val graph = SimpleDomain.newGraph
      val node1 = graph + Thing.Label
      val node2 = graph + Thing.Label
      node1 --- Connection.Label --> node2

      node1.out(Connection.Label).next shouldBe node2
    }

    "add an edge with one property" in {
      val graph = SimpleDomain.newGraph
      val node1 = graph + Thing.Label
      val node2 = graph + Thing.Label
      node1 --- (Connection.Label, Distance -> 10) --> node2

      node1.out(Connection.Label).next shouldBe node2
      node1.outE(Connection.Label).property(Connection.Properties.Distance).next() shouldBe 10
      node1.outE(Connection.Label).property[Int](Connection.PropertyNames.Distance).next() shouldBe 10
    }

    "add an edge with multiple properties" in {
      import Connection.Properties._
      val graph = SimpleDomain.newGraph
      val node1 = graph + Thing.Label
      val node2 = graph + Thing.Label
      node1 --- (Connection.Label, Distance.of(10)) --> node2
      node1 --- (Connection.Label, Distance.of(30), Name.of("Alternative")) --> node2

      node1.out(Connection.Label).toList shouldBe List(node2, node2)
      node1.outE(Connection.Label).propertiesMap.toSetMutable shouldBe Set(
        Map(("distance", 10)),
        Map(("distance", 30), ("name", "Alternative"))
      )
    }
  }

}
