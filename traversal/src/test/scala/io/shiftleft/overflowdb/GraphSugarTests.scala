package io.shiftleft.overflowdb

import io.shiftleft.overflowdb.traversal._
import io.shiftleft.overflowdb.traversal.testdomains.simple.{Connection, SimpleDomain, Thing}
import org.scalatest.{Matchers, WordSpec}

class GraphSugarTests extends WordSpec with Matchers {

  "graph + label" can {
    "add a node" in {
      val graph = SimpleDomain.newGraph
      graph + Thing.Label
      graph.nodeCount shouldBe 1
    }

    "add a node with properties" in {
      val graph = SimpleDomain.newGraph
      graph + (Thing.Label, Thing.Properties.Name -> "one thing")
      graph.nodeCount shouldBe 1
      SimpleDomain.traversal(graph).things.name.toList shouldBe List("one thing")
    }

    "fail for unknown nodeType" in {
      val graph = SimpleDomain.newGraph
      intercept[IllegalArgumentException] {
        graph + "unknown"
      }
      graph.nodeCount shouldBe 0
    }
  }

  "arrow syntax" can {
    "add an edge" in {
      val graph = SimpleDomain.newGraph
      val node1 = graph + Thing.Label
      val node2 = graph + Thing.Label
      node1 --- Connection.Label --> node2

      graph.nodeCount shouldBe 2
      node1.out(Connection.Label).next shouldBe node2
    }

    "add an edge with properties" in {
      val graph = SimpleDomain.newGraph
      val node1 = graph + Thing.Label
      val node2 = graph + Thing.Label
      node1 --- (Connection.Label, Connection.Properties.Distance -> 10) --> node2

      graph.nodeCount shouldBe 2
      node1.out(Connection.Label).next shouldBe node2

      node1.outE(Connection.Label).property(Connection.Properties.Distance).next shouldBe 10
//      node1.outE(Connection.Label).next.property(Connection.PropertyNames.Distance) shouldBe 10
    }
  }

}
