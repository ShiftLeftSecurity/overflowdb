package io.shiftleft.overflowdb.traversal

import scala.collection.{IterableFactory, IterableFactoryDefaults, IterableOnce, IterableOps, Iterator, mutable}


class Traversal2[A](elements: IterableOnce[A]) extends IterableOnce[A] with IterableOps[A, Traversal2, Traversal2[A]]
  with IterableFactoryDefaults[A, Traversal2] {
  def next: A = iterator.next()
  def hasNext: Boolean = iterator.hasNext

  override val iterator: Iterator[A] = new Iterator[A] {
    private val wrappedIter = elements.iterator
    private var isExhausted = false

    override def hasNext: Boolean = {
      val _hasNext = wrappedIter.hasNext
      if (!_hasNext) {
        if (isExhausted) println("warning: Traversal already exhausted")
        else isExhausted = true
      }
      _hasNext
    }

    override def next(): A = wrappedIter.next
  }
  override def iterableFactory: IterableFactory[Traversal2] = Traversal2
  override def toIterable: Iterable[A] = ???
  override protected def coll: Traversal2[A] = this

  def repeat(repeatTraversal: Traversal2[A] => Traversal2[A], times: Int): Traversal2[A] = {
    _repeat(repeatTraversal, times)
  }

  private def _repeat(repeatTraversal: Traversal2[A] => Traversal2[A], times: Int): Traversal2[A] = {
    if (times > 0 && iterator.hasNext) {
      repeatTraversal(this)._repeat(repeatTraversal, times -1)
    } else this
  }

}

object Traversal2 extends IterableFactory[Traversal2]{
  override def from[A](source: IterableOnce[A]): Traversal2[A] = new Traversal2(Iterator.from(source))
  override def empty[A]: Traversal2[A] = from(Iterator.empty)
  override def newBuilder[A]: mutable.Builder[A, Traversal2[A]] = Iterator.newBuilder[A].mapResult(new Traversal2(_))

  def fromSingle[A](a: A): Traversal2[A] = new Traversal2(Iterator.single(a))
}

object Traversal12Usage extends App {
//  println("creating traversal2")
//  val traversal2 = Iterator("A", "B", "C").to(Traversal2)
//    .repeat(_.map{ // .collect makes it eager!
//      case s => val res = s.appended('.')
//        println(s"repeatTraversal2 collect: $res")
//        res
//    }, 4)

//  println("creating traversal1a")
//  val traversal1a = Iterator("A", "B", "C").to(Traversal)
//    .repeat(_.collect { // .collect makes it eager, if s.length < 4  makes it extra-eager!
//      case s if s.length < 4 =>
//        val res = s.appended('.')
//        println(s"repeatTraversal1a collect: $res")
//        res
//    }, _.maxDepth(3))

//  println("creating traversal1b")
//  val traversal1b = Iterator("A", "B", "C").to(Traversal)
//    .repeat(_.map { s =>
//      val res = s.appended('.')
//      println(s"repeatTraversal1b map: $res")
//      res
//    }, _.maxDepth(3))


  //  assert(traversal2.next == "A..")
  //    assert(traversal1.next == "A...")

//  println("creating traversal1c")
//  val traversal1b = Iterator("A", "B", "C").to(Traversal)
//    .repeat(_.filter { s => println(s"in filter: $s"); true }
//      , _.maxDepth(3))

  val iter = new Iterator[Int] {
    var i = 0
    override def hasNext: Boolean = {
      val res = i < 4
      println(s"iter.hasNext: $res")
      res
    }
    override def next(): Int = {
      val res = i
      i+=1
      println(s"iter.next: $res")
      res
    }
  }
  val trav = iter.to(Traversal)
  val repeatTrav = trav.repeat(
    _.map { i =>
      val res = i + 10
      println(s"in map: $res")
      res
    }
     .filter { i => println(s"in filter: $i"); i < 50 },
    _.times(2)
  )

  println("repeatTrav created")
  println(repeatTrav.hasNext)
  println(repeatTrav.next)
}

object Traversal2Usage extends App {
  val initialTraversal: Traversal2[String] = List("A", "B", "C").to(Traversal2)
  val repeatTraversal: Traversal2[String] => Traversal2[String] = {
    trav: Traversal2[String] => new Traversal2[String](trav.iterator.collect {
      case s if s.length < 4 =>
        val res = s.appended('.')
        println(s"repeatTraversal collect: $res")
        res
    })
  }

  val mainTraversal = initialTraversal.repeat(repeatTraversal, 2)
//  assert(mainTraversal.next == "A..")
  /* expected output:
  repeat flatMap: A
  repeatTraversal collect: A.
  repeatTraversal collect: A..
   */
//  assert(mainTraversal.next == "B..")
  /* expected output:
  repeat flatMap: B
  repeatTraversal collect: B.
  repeatTraversal collect: B..
   */
}

object TraversalUsage extends App {
  val initialTraversal: Traversal[String] = List("A", "B", "C").to(Traversal)
//  val repeatTraversal: Traversal[String] => Traversal[String] = {
//    trav: Traversal[String] =>
//      new Traversal[String](trav.iterator.collect {
//        case s if s.length < 4 =>
//          val res = s.appended('.')
//          println(s"repeatTraversal collect: $res")
//          res
//      })
//  }
//
  val t1 = initialTraversal.repeat(
    _.collect {
      case s if s.length < 40 =>
        val res = s.appended('.')
        println(s"repeatTraversal collect: $res")
        res
    }, _.times(4))

//  assert(t1.next == "A..")
//  assert(t1.next == "B..")
//  assert(t1.next == "C..")
//  assert(!t1.hasNext)

//  val t2 = initialTraversal.collect { s =>
//    val res = s.appended('.')
//    println(s"inside map: $res")
//    res
//  }
//  println(t2.emptyCheck)
//  println(t2.isEmpty)
//  println(t2.next)
}

//      this.flatMap { a =>
//        println(s"repeat flatMap: $a")
//        val aLifted = Traversal2.fromSingle(a)
////        repeatTraversal(aLifted) //lazy, but only one step (no actual repetition...), and BFS
//        repeatTraversal(aLifted).repeat(repeatTraversal, times - 1) // lazy for traversal itself, but eagerly iterates the entire collection once head is accessed - actually that's in line with what gremlin does...
//      }