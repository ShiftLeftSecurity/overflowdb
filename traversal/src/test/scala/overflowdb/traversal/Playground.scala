package overflowdb.traversal

import overflowdb._
import overflowdb.traversal.Foo3.{a, repeatCount}
import overflowdb.traversal.testdomains.simple.ExampleGraphSetup._
import overflowdb.traversal.testdomains.simple.Thing.Properties.Name

object Foo3 extends App {
  import overflowdb.traversal.testdomains.simple._
  import Thing.Properties.Name
  val graph = SimpleDomain.newGraph
  def addThing(name: String) = graph + (Thing.Label, Name -> name)

  val a = addThing("a")
  val b = addThing("b")
  val c = addThing("c")
  a --- Connection.Label --> b
  b --- Connection.Label --> c
  c --- Connection.Label --> a

  //  val repeatCount = 0
//    val repeatCount = 1
//    val repeatCount = 2
//    val repeatCount = 3001
//    val repeatCount = 9001 // fails with StackOverflowError in my repeat version - up to repeat5
  val repeatCount = 90002 // becomes super slow with TP!

//  val odbTrav = Traversal.fromSingle(a).repeat7(_.out, repeatCount).property(Name)
//  println(s"repeat7: ${odbTrav.head}")

  val odbTrav = Traversal.fromSingle(a).repeatDfs(_.out, repeatCount).property(Name)
  println(s"repeatDfs: ${odbTrav.head}")

  // TP3
//  import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.__
//  import org.apache.tinkerpop.gremlin.process.traversal.{Traversal => TPTraversal}
//  println("tp3: " + __(a).repeat(__().out().asInstanceOf[TPTraversal[_, Node]]).times(repeatCount).values(Name.name).next())
}

object Foo4 extends App {
  import scala.jdk.CollectionConverters._
  val repeatCount = 3
  // todo why not this simple?
  //  val odbTrav = Traversal.fromSingle(centerNode).repeat7(_.out, repeatCount).property(Name)
  //  val odbTrav = Traversal.fromSingle(centerNode).repeat7(thing => thing.out.asScala.to(Traversal), repeatCount).property(Name)
//  val odbTravBfs = Traversal.fromSingle(centerNode: Node).repeat7(t => {println(t.property2(Name.name)); t.out}, repeatCount).property(Name)
//  println(s"odb bfs: ${odbTravBfs.head}")
  /* output shows: it's BFS:
   * Center L1 R1 L2 R2
   * odb bfs: L3
   */

//  println(Traversal.fromSingle(centerNode).l)
//  val trav = Traversal.fromSingle(centerNode: Node).repeatX.property(Name)
//  println(trav.next)
//  println(trav.next)

//  val odbTravDfs = Traversal.fromSingle(centerNode: Node).repeatDfs(t => {println(t.property2(Name.name)); t.out}, repeatCount).property(Name)
  val odbTravDfs = Traversal.fromSingle(centerNode: Node).repeatDfs(_.out, repeatCount).property(Name)
//  println(s"odb dfs: ${odbTravDfs.head}")
//  println(s"odb dfs: ${odbTravDfs.head}")
//  println(s"odb dfs: ${odbTravDfs.head}")
//  println(s"odb dfs: ${odbTravDfs.head}")
//  println(s"odb dfs: ${odbTravDfs.head}")
    println(s"odb dfs: ${odbTravDfs.l}")

  // original repeat is DFS - but runs entirely on stack, i.e. has issues with stack size...
//  val odbOrig = Traversal.fromSingle(centerNode: Node).repeat(t => {println(t.property(Name.name)); t.out})(_.times(repeatCount)).property(Name)
//  println(s"odb dfs: ${odbOrig.head}")


//  println("now TP3")
//  import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.__
//  import org.apache.tinkerpop.gremlin.process.traversal.Traverser
//  import org.apache.tinkerpop.gremlin.process.traversal.{Traversal => TPTraversal}
//  println("tp3: " + __(centerNode: Node).repeat(__().sideEffect{x: Traverser[Node] => println(x.get.value(Name.name))}.out().asInstanceOf[TPTraversal[_, Node]]).times(repeatCount).values(Name.name).next())
//  // also BFS


}
