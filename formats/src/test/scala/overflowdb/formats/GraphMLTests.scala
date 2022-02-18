package overflowdb.formats

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import overflowdb.testdomains.gratefuldead.GratefulDead

class GraphMLTests extends AnyWordSpec {

  "import minified gratefuldead graph" in {
    val graph = GratefulDead.newGraph()
    graph.nodeCount() shouldBe 0

    GraphML.insert("formats/src/test/resources/graphml-small.xml", graph)
    graph.nodeCount() shouldBe 3
    graph.edgeCount() shouldBe 2

    val node1 = graph.node(1)
    node1.label() shouldBe "song"
    val node340 = node1.out("sungBy").next()
    val node527 = node1.out("writtenBy").next()

    node340.label shouldBe "artist"
    node340.property("name") shouldBe "Garcia"
    node340.out().hasNext shouldBe false
    node340.in().hasNext shouldBe true

    node527.label shouldBe "artist"
    node527.property("name") shouldBe "Bo_Diddley"
    node527.out().hasNext shouldBe false
    node527.in().hasNext shouldBe true

    graph.close()
  }
  
}
