package overflowdb.traversal

import overflowdb.NodeRef
import overflowdb.traversal.testdomains.simple.{Connection, ExampleGraphSetup, Thing}
import org.scalatest.{Matchers, WordSpec}

/** generic graph traversals, i.e. domain independent */
class GenericGraphTraversalTests extends WordSpec with Matchers {
  import ExampleGraphSetup._

  "V for all nodes" in {
    graph.V.count shouldBe 8
    graph.V.size shouldBe 8
  }

  "E for all edges" in {
    graph.E.count shouldBe 7
    graph.E.size shouldBe 7
  }

  "label lookup" in {
    graph.V.label.toList shouldBe List("thing", "thing", "thing", "thing", "thing", "thing", "thing", "thing")
    graph.E.label.toList shouldBe List("connection", "connection", "connection", "connection", "connection", "connection", "connection")
  }

  "property lookup" in {
    graph.V.property(Thing.Properties.Name).toSet shouldBe Set("L3", "L2", "L1", "Center", "R1", "R2", "R3", "R4")
    graph.E.property(Connection.Properties.Distance).toSet shouldBe Set(10, 13)
    graph.E.propertyOption(Connection.Properties.Distance).toSet shouldBe Set(Some(10), Some(13), None)
  }

  "filter steps" can {
    "filter by id" in {
      graph.V.hasId(centerNode.id).property(Thing.Properties.Name).toList shouldBe List("Center")
    }

    "filter by label" in {
      graph.V.hasLabel(Thing.Label).size shouldBe 8
      graph.V.hasLabel(nonExistingLabel).size shouldBe 0
      graph.E.hasLabel(Connection.Label).size shouldBe 7
      graph.E.hasLabel(nonExistingLabel).size shouldBe 0
    }

    "filter by property key" in {
      graph.V.has(Thing.Properties.Name).size shouldBe 8
      graph.V.has(nonExistingPropertyKey).size shouldBe 0
      graph.V.hasNot(Thing.Properties.Name).size shouldBe 0
      graph.V.hasNot(nonExistingPropertyKey).size shouldBe 8

      graph.E.has(Connection.Properties.Distance).size shouldBe 3
      graph.E.hasNot(Connection.Properties.Distance).size shouldBe 4
    }

    "filter by property key/value" in {
      graph.V.has(Thing.Properties.Name -> "R1").size shouldBe 1
      graph.V.hasNot(Thing.Properties.Name -> "R1").size shouldBe 7
      graph.E.has(Connection.Properties.Distance -> 10).size shouldBe 2
      graph.E.hasNot(Connection.Properties.Distance -> 10).size shouldBe 5
    }

    "`where` step taking a traversal" in {
      // find all nodes that _do_ have an OUT neighbor, i.e. find the inner nodes
      graph.V.where(_.out).property(Thing.Properties.Name).toSet shouldBe Set("L2", "L1", "Center", "R1", "R2", "R3")
    }

    "`not` step taking a traversal" in {
      // find all nodes that do _not_ have an OUT neighbor, i.e. find the outermost nodes
       graph.V.not(_.out).property(Thing.Properties.Name).toSet shouldBe Set("L3", "R4")
    }
  }

  "base steps: out/in/both" can {
    "step out" in {
      assertNames(centerTrav.out, Set("L1", "R1"))
      assertNames(centerNode.out, Set("L1", "R1"))
      assertNames(centerTrav.out.out, Set("L2", "R2"))
      assertNames(centerNode.out.out, Set("L2", "R2"))
      assertNames(centerTrav.out(Connection.Label), Set("L1", "R1"))
      assertNames(centerNode.out(Connection.Label), Set("L1", "R1"))
      assertNames(centerTrav.out(nonExistingLabel), Set.empty)
      assertNames(centerNode.out(nonExistingLabel), Set.empty)
    }

    "step in" in {
      l2Trav.in.size shouldBe 1
      l2Node.in.size shouldBe 1
      assertNames(l2Trav.in, Set("L1"))
      assertNames(l2Node.in, Set("L1"))
      assertNames(l2Trav.in.in, Set("Center"))
      assertNames(l2Node.in.in, Set("Center"))
      assertNames(l2Trav.in(Connection.Label), Set("L1"))
      assertNames(l2Node.in(Connection.Label), Set("L1"))
      assertNames(l2Trav.in(nonExistingLabel), Set.empty)
      assertNames(l2Node.in(nonExistingLabel), Set.empty)
    }

    "step both" in {
      /* L3 <- L2 <- L1 <- Center -> R1 -> R2 -> R3 -> R4 */
      l2Trav.both.size shouldBe 2
      l2Node.both.size shouldBe 2
      assertNames(l2Trav.both, Set("L1", "L3"))
      assertNames(l2Node.both, Set("L1", "L3"))
      assertNames(r2Trav.both, Set("R1", "R3"))
      assertNames(r2Node.both, Set("R1", "R3"))
      assertNames(l2Trav.both.both, Set("L2", "Center"))
      assertNames(l2Node.both.both, Set("L2", "Center"))
      assertNames(r2Trav.both.both, Set("Center", "R2", "R4"))
      assertNames(r2Node.both.both, Set("Center", "R2", "R4"))
      assertNames(l2Trav.both(Connection.Label), Set("L1", "L3"))
      assertNames(l2Node.both(Connection.Label), Set("L1", "L3"))
      assertNames(l2Trav.both(nonExistingLabel), Set.empty)
      assertNames(l2Node.both(nonExistingLabel), Set.empty)
    }

    "step outE" in {
      centerTrav.outE.size shouldBe 2
      centerNode.outE.size shouldBe 2
      assertNames(centerTrav.outE.inV, Set("L1", "R1"))
      assertNames(centerNode.outE.inV, Set("L1", "R1"))
      assertNames(centerTrav.outE.inV.outE.inV, Set("L2", "R2"))
      assertNames(centerNode.outE.inV.outE.inV, Set("L2", "R2"))
      assertNames(centerTrav.outE(Connection.Label).inV, Set("L1", "R1"))
      assertNames(centerNode.outE(Connection.Label).inV, Set("L1", "R1"))
      assertNames(centerTrav.outE(nonExistingLabel).inV, Set.empty)
      assertNames(centerNode.outE(nonExistingLabel).inV, Set.empty)
    }

    "step inE" in {
      l2Trav.inE.size shouldBe 1
      l2Node.inE.size shouldBe 1
      assertNames(l2Trav.inE.outV, Set("L1"))
      assertNames(l2Node.inE.outV, Set("L1"))
      assertNames(l2Trav.inE.outV.inE.outV, Set("Center"))
      assertNames(l2Node.inE.outV.inE.outV, Set("Center"))
      assertNames(l2Trav.inE(Connection.Label).outV, Set("L1"))
      assertNames(l2Node.inE(Connection.Label).outV, Set("L1"))
      assertNames(l2Trav.inE(nonExistingLabel).outV, Set.empty)
      assertNames(l2Node.inE(nonExistingLabel).outV, Set.empty)
    }

    "step bothE" in {
      /* L3 <- L2 <- L1 <- Center -> R1 -> R2 -> R3 -> R4 */
      l2Trav.bothE.size shouldBe 2
      l2Node.bothE.size shouldBe 2
      l2Trav.bothE(Connection.Label).size shouldBe 2
      l2Node.bothE(Connection.Label).size shouldBe 2
      l2Trav.bothE(nonExistingLabel).size shouldBe 0
      l2Node.bothE(nonExistingLabel).size shouldBe 0
    }
  }

  def assertNames[A <: NodeRef[_]](traversal: Traversal[A], expectedNames: Set[String]) = {
    traversal.property(Thing.Properties.Name).toSet shouldBe expectedNames
  }
}
