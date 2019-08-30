import io.shiftleft.overflowdb.testdomains.gratefuldead.GratefulDead

object OverflowDb extends App {
  Benchmark.benchmark(GratefulDead.newGraph)
}
