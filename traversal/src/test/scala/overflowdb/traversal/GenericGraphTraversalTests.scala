package overflowdb.traversal

import org.scalatest.{Matchers, WordSpec}
import overflowdb.traversal.filter.P
import overflowdb.traversal.testdomains.simple.Connection.Properties.Distance
import overflowdb.traversal.testdomains.simple.Thing.Properties.Name
import overflowdb.traversal.testdomains.simple.{Connection, ExampleGraphSetup, Thing}

/** generic graph traversals, i.e. domain independent */
class GenericGraphTraversalTests extends WordSpec with Matchers {
  import ExampleGraphSetup._

  "V for all nodes" in {
    graph.V.count.head shouldBe 8
    graph.V.size shouldBe 8
  }

  "E for all edges" in {
    graph.E.count.head shouldBe 7
    graph.E.size shouldBe 7
  }

  "label lookup" in {
    graph.V.label.toList shouldBe List("thing", "thing", "thing", "thing", "thing", "thing", "thing", "thing")
    graph.E.label.toList shouldBe List("connection", "connection", "connection", "connection", "connection", "connection", "connection")
  }

  "property lookup" in {
    graph.V.property(Name).toSet shouldBe Set("L3", "L2", "L1", "Center", "R1", "R2", "R3", "R4")
    graph.E.property(Distance).toSet shouldBe Set(10, 13)
    graph.E.propertyOption(Distance).toSet shouldBe Set(Some(10), Some(13), None)
  }

  "filter steps" can {
    "filter by id" in {
      graph.V.hasId(center.id2).property(Name).toList shouldBe List("Center")
    }

    "filter by label" in {
      graph.V.label(Thing.Label).size shouldBe 8
      graph.V.label(nonExistingLabel).size shouldBe 0
      graph.V.label(Thing.Label, nonExistingLabel).size shouldBe 8
      graph.V.labelNot(nonExistingLabel).size shouldBe 8
      graph.V.labelNot(Thing.Label, nonExistingLabel).size shouldBe 0

      graph.E.label(Connection.Label).size shouldBe 7
      graph.E.label(nonExistingLabel).size shouldBe 0
      graph.E.label(Connection.Label, nonExistingLabel).size shouldBe 7
      graph.E.labelNot(nonExistingLabel).size shouldBe 7
      graph.E.labelNot(Connection.Label, nonExistingLabel).size shouldBe 0
    }

    "filter by property key" in {
      graph.V.has(Name).size shouldBe 8
      graph.V.has(nonExistingPropertyKey).size shouldBe 0
      graph.V.hasNot(Name).size shouldBe 0
      graph.V.hasNot(nonExistingPropertyKey).size shouldBe 8

      graph.E.has(Distance).size shouldBe 3
      graph.E.hasNot(Distance).size shouldBe 4
    }

    "filter by property key/value" in {
      graph.V.has(Name, "R1").size shouldBe 1
      graph.V.has(Name -> "R1").size shouldBe 1
      graph.V.has(Name.where(P.eq("R1"))).size shouldBe 1
      graph.V.has(Name.where(P.matches("[LR]."))).size shouldBe 7
      graph.V.has(Name.where(_.matches("[LR]."))).size shouldBe 7
      graph.V.has(Name.where(P.neq("R1"))).size shouldBe 7
      graph.V.has(Name.where(P.within(Set("L1", "L2")))).size shouldBe 2
      graph.V.has(Name.where(P.within("L1", "L2", "L3"))).size shouldBe 3
      graph.V.has(Name.where(P.without(Set("L1", "L2")))).size shouldBe 6
      graph.V.has(Name.where(P.without("L1", "L2", "L3"))).size shouldBe 5
      graph.V.has(Name.where(_.endsWith("1"))).size shouldBe 2
      graph.E.has(Distance -> 10).size shouldBe 2

      graph.V.hasNot(Name, "R1").size shouldBe 7
      graph.V.hasNot(Name -> "R1").size shouldBe 7
      graph.V.hasNot(Name.where(P.eq("R1"))).size shouldBe 7
      graph.V.hasNot(Name.where(P.matches("[LR]."))).size shouldBe 1
      graph.V.hasNot(Name.where(_.matches("[LR]."))).size shouldBe 1
      graph.V.hasNot(Name.where(P.neq("R1"))).size shouldBe 1
      graph.V.hasNot(Name.where(P.within(Set("L1", "L2")))).size shouldBe 6
      graph.V.hasNot(Name.where(P.within("L1", "L2", "L3"))).size shouldBe 5
      graph.V.hasNot(Name.where(P.without(Set("L1", "L2")))).size shouldBe 2
      graph.V.hasNot(Name.where(P.without("L1", "L2", "L3"))).size shouldBe 3
      graph.V.hasNot(Name.where(_.endsWith("1"))).size shouldBe 6
      graph.E.hasNot(Distance -> 10).size shouldBe 5
    }

    "`where` step taking a traversal" in {
      // find all nodes that _do_ have an OUT neighbor, i.e. find the inner nodes
      graph.V.where(_.out).property(Name).toSet shouldBe Set("L2", "L1", "Center", "R1", "R2", "R3")
    }

    "`not` step taking a traversal" in {
      // find all nodes that do _not_ have an OUT neighbor, i.e. find the outermost nodes
       graph.V.not(_.out).property(Name).toSet shouldBe Set("L3", "R4")
    }
  }

  "base steps: out/in/both" can {
    "step out" in {
      center.start.out.toSet shouldBe Set(l1, r1)
      center.start.out.out.toSet shouldBe Set(l2, r2)
      center.start.out(Connection.Label).toSet shouldBe Set(l1, r1)
      center.start.out(nonExistingLabel, Connection.Label).toSet shouldBe Set(l1, r1)
      center.start.out(nonExistingLabel).toSet shouldBe Set.empty
    }

    "step in" in {
      l2.start.in.size shouldBe 1
      l2.start.in.toSet shouldBe Set(l1)
      l2.start.in.in.toSet shouldBe Set(center)
      l2.start.in(Connection.Label).toSet shouldBe Set(l1)
      l2.start.in(nonExistingLabel, Connection.Label).toSet shouldBe Set(l1)
      l2.start.in(nonExistingLabel).toSet shouldBe Set.empty
    }

    "step both" in {
      /* L3 <- L2 <- L1 <- Center -> R1 -> R2 -> R3 -> R4 */
      l2.start.both.size shouldBe 2
      l2.start.both.toSet shouldBe Set(l1, l3)
      r2.start.both.toSet shouldBe Set(r1, r3)
      l2.start.both.both.toSet shouldBe Set(l2, center)
      r2.start.both.both.toSet shouldBe Set(center, r2, r4)
      l2.start.both(Connection.Label).toSet shouldBe Set(l1, l3)
      l2.start.both(nonExistingLabel, Connection.Label).toSet shouldBe Set(l1, l3)
      l2.start.both(nonExistingLabel).toSet shouldBe Set.empty
    }

    "step outE" in {
      center.start.outE.size shouldBe 2
      center.start.outE.inV.toSet shouldBe Set(l1, r1)
      center.start.outE.inV.outE.inV.toSet shouldBe Set(l2, r2)
      center.start.outE(Connection.Label).inV.toSet shouldBe Set(l1, r1)
      center.start.outE(nonExistingLabel, Connection.Label).inV.toSet shouldBe Set(l1, r1)
      center.start.outE(nonExistingLabel).inV.toSet shouldBe Set.empty
    }

    "step inE" in {
      l2.start.inE.size shouldBe 1
      l2.start.inE.outV.toSet shouldBe Set(l1)
      l2.start.inE.outV.inE.outV.toSet shouldBe Set(center)
      l2.start.inE(Connection.Label).outV.toSet shouldBe Set(l1)
      l2.start.inE(nonExistingLabel, Connection.Label).outV.toSet shouldBe Set(l1)
      l2.start.inE(nonExistingLabel).outV.toSet shouldBe Set.empty
    }

    "step bothE" in {
      /* L3 <- L2 <- L1 <- Center -> R1 -> R2 -> R3 -> R4 */
      l2.start.bothE.size shouldBe 2
      l2.start.bothE(Connection.Label).size shouldBe 2
      l2.start.bothE(nonExistingLabel, Connection.Label).size shouldBe 2
      l2.start.bothE(nonExistingLabel).size shouldBe 0
    }
  }

}
