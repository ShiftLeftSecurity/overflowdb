package io.shiftleft.overflowdb.traversals

import io.shiftleft.overflowdb.testdomains.gratefuldead.{GratefulDead, GratefulDeadTraversalSource}
import org.scalatest.{Matchers, WordSpec}

import scala.jdk.CollectionConverters._

class TraveralTest extends WordSpec with Matchers {
  "simple and basic" in {
    val graph = GratefulDead.newGraphWithData
    println(new Traversal(graph.vertices().asScala).size)
  }

  "grateful dead traversals" in {
    val graph = GratefulDead.newGraphWithData
//    GratefulDead.traversal(graph) // only intellij get's this wrong... clear caches?
    val gratefulDead = new GratefulDeadTraversalSource(graph)

    gratefulDead.all.size shouldBe 808
    gratefulDead.artists.size shouldBe 224
    gratefulDead.songs.size shouldBe 584

    val artistNames = gratefulDead.artists.name.l
    artistNames.size shouldBe 224
    artistNames.contains("Bob_Dylan") shouldBe true
  }

}
