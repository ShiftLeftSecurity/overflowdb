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
      gratefulDead.artists.nameExact("Bob_Dylan").size shouldBe 1

      val artistNames = gratefulDead.artists.name.l
      artistNames.size shouldBe 224
      artistNames.contains("Bob_Dylan") shouldBe true

      gratefulDead.artists.nameExact("Bob_Dylan").size shouldBe 1
      gratefulDead.artists.nameExact("Bob_Dylan", "All").size shouldBe 2
      gratefulDead.artists.name(".*").size shouldBe 224
      gratefulDead.artists.name(".*Bob.*").size shouldBe 3
      gratefulDead.artists.name(".*Bob.*", "^M.*").size shouldBe 16
      gratefulDead.artists.nameNot(".*Bob.*").size shouldBe 221
    }

    "traverse domain-specific edges" in {
      gratefulDead.artists.nameExact("Bob_Dylan").sangSongs.size shouldBe 22
      gratefulDead.songs.nameExact("WALKIN THE DOG").followedBy.size shouldBe 5
      gratefulDead.songs.nameExact("WALKIN THE DOG").followedBy.songType.toSet shouldBe Set("original", "cover", "")
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

  "lifting elements into a Traversal" can {
    "lift a single element with `Traversal.fromSingle`" in {
      val dylan = gratefulDead.artists.nameExact("Bob_Dylan").head
      Traversal.fromSingle(dylan).sangSongs.size shouldBe 22
    }

    "lift a single element with `.start`" in {
      val dylan = gratefulDead.artists.nameExact("Bob_Dylan").head
      dylan.start.sangSongs.size shouldBe 22
    }

    "lift multiple elements with `Traversal.from`" in {
      val artists = gratefulDead.artists.nameExact("Bob_Dylan", "All").toList
      Traversal.from(artists).sangSongs.size shouldBe 31
    }
  }
}
