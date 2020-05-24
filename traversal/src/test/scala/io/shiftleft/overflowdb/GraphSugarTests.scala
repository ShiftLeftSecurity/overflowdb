package io.shiftleft.overflowdb

import io.shiftleft.overflowdb.traversal.testdomains.simple.{Connection, SimpleDomain, Thing}
import org.scalatest.{Matchers, WordSpec}

class GraphSugarTests extends WordSpec with Matchers {

  "graph + label" can {
    "add a node" in {
      val graph = SimpleDomain.newGraph
      graph + Thing.Label
      graph.nodeCount shouldBe 1
    }

    "or not" in {
      val graph = SimpleDomain.newGraph
      intercept[IllegalArgumentException] {
        graph + "unknown"
      }
      graph.nodeCount shouldBe 0
    }
  }

  "arrow syntax" can {
    "add an edge: `node --- edge --> node`" in {
      val graph = SimpleDomain.newGraph
      val node1 = graph + Thing.Label
      val node2 = graph + Thing.Label
      node1 --- Connection.Label --> node2

      graph.nodeCount shouldBe 2
      node1.nodesOut(Connection.Label).next shouldBe node2
    }
  }

}
