package io.shiftleft.overflowdb.traversals

import io.shiftleft.overflowdb.traversals.testdomains.gratefuldead._
import org.scalatest.{Matchers, WordSpec}

class GratefulDeadTests extends WordSpec with Matchers {
  val gratefulDead = GratefulDead.traversal(GratefulDead.newGraphWithData)

  "generic graph traversals" in {
    gratefulDead.all.size shouldBe 808
    gratefulDead.all.id.l.sorted.head shouldBe 1
    gratefulDead.all.label.toSet shouldBe Set(Artist.Label, Song.Label)

    gratefulDead.withLabel(Artist.Label).size shouldBe 224
    gratefulDead.withId(1).label.head shouldBe Song.Label
    gratefulDead.withId(2).property[String](Song.Properties.Name).head shouldBe "IM A MAN"
    gratefulDead.withIds(3, 4).property[String](Song.Properties.Name).l shouldBe Seq("BERTHA", "NOT FADE AWAY")
  }

  "domain specific traversals" can {
    "perform single step start traversals" in {
      gratefulDead.artists.size shouldBe 224
      gratefulDead.songs.size shouldBe 584
    }

    "perform property related traversals" in {
      gratefulDead.artists.name("Bob_Dylan").size shouldBe 1

      val artistNames = gratefulDead.artists.name.l
      artistNames.size shouldBe 224
      artistNames.contains("Bob_Dylan") shouldBe true
    }

    "traverse domain-specific edges" in {
      gratefulDead.artists.name("Bob_Dylan").sangSongs.size shouldBe 22
      gratefulDead.songs.name("WALKIN THE DOG").followedBy.size shouldBe 5
      gratefulDead.songs.name("WALKIN THE DOG").followedBy.songType.toSet shouldBe Set("original", "cover", "")
    }

    "be expressed in for comprehension" in {
      val traversal = for {
        artist <- gratefulDead.artists
        song <- artist.sangSongs
      } yield artist.name -> song.name

      val artistAndSongTuples = traversal.l
      artistAndSongTuples.size shouldBe 501
      artistAndSongTuples.sortBy(_._1).head shouldBe ("All" -> "AND WE BID YOU GOODNIGHT")
    }
  }

  "lift a node into a Traversal with .start" in {
    val dylan = gratefulDead.artists.name("Bob_Dylan").head
    dylan.start.sangSongs.size shouldBe 22
  }

}