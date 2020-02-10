package overflowdb.traversal

import overflowdb.traversal.filter.StringPropertyFilter.InvalidRegexException
import overflowdb.traversal.testdomains.gratefuldead._
import org.scalatest.{Matchers, WordSpec}

class GratefulDeadTests extends WordSpec with Matchers {
  val gratefulDead = GratefulDead.traversal(GratefulDead.newGraphWithData)

  "generic graph traversal" can {
    "perform generic graph steps" in {
      gratefulDead.all.size shouldBe 808
      gratefulDead.all.id.l.sorted.head shouldBe 1
      gratefulDead.all.label.toSet shouldBe Set(Artist.Label, Song.Label)

      gratefulDead.label(Artist.Label).size shouldBe 224
      gratefulDead.id(1).label.head shouldBe Song.Label
      gratefulDead.id(2).property(Song.Properties.Name).head shouldBe "IM A MAN"
      gratefulDead.ids(3, 4).property[String]("name").l shouldBe Seq("BERTHA", "NOT FADE AWAY")
      gratefulDead.all.hasProperty(Song.Properties.SongType).size shouldBe 584
      gratefulDead.all.hasProperty(Song.Properties.Performances, 2).size shouldBe 36
    }
  }

  "domain specific traversal" can {
    "perform single step start traversal" in {
      gratefulDead.artists.size shouldBe 224
      gratefulDead.songs.size shouldBe 584
    }

    "perform property related traversal" in {
      gratefulDead.artists.nameExact("Bob_Dylan").size shouldBe 1

      val artistNames = gratefulDead.artists.name.l
      artistNames.size shouldBe 224
      artistNames.contains("Bob_Dylan") shouldBe true
    }

    "property filter" in {
      gratefulDead.artists.name(".*Bob.*").size shouldBe 3
      gratefulDead.artists.name(".*Bob.*", "^M.*").size shouldBe 16
      gratefulDead.artists.nameNot(".*Bob.*").size shouldBe 221
      gratefulDead.artists.nameNot(".*Bob.*", "^M.*").size shouldBe 208
      gratefulDead.artists.nameExact("Bob_Dylan").size shouldBe 1
      gratefulDead.artists.nameExact("Bob_Dylan", "All").size shouldBe 2
      gratefulDead.artists.nameStartsWith("Bob").size shouldBe 3
      gratefulDead.artists.nameEndsWith("Dylan").size shouldBe 1
      gratefulDead.artists.nameContains("M").size shouldBe 30
      gratefulDead.artists.nameContainsNot("M").size shouldBe 194

      gratefulDead.songs.performances(1).size shouldBe 142
      gratefulDead.songs.performancesGt(1).size shouldBe 341
      gratefulDead.songs.performancesGte(1).size shouldBe 483
      gratefulDead.songs.performancesLt(1).size shouldBe 101
      gratefulDead.songs.performancesLte(1).size shouldBe 243
    }

    "throw useful exception when passing invalid regexp" in {
      intercept[InvalidRegexException] { gratefulDead.artists.name("this regexp is invalid [") }
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
