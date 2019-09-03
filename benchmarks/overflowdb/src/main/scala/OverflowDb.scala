import io.shiftleft.overflowdb.testdomains.gratefuldead.GratefulDead

object OverflowDbTinkerpop3 extends App {
  Benchmarks.Tinkerpop3.benchmark(GratefulDead.newGraph)
}

