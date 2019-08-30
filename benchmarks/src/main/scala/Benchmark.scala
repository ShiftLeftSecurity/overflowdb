object Benchmark {

  def timed(msg: String, iterations: Int, warmupIterations: Int = 1)(fun: () => Unit): Unit = {
    1.to(warmupIterations).foreach { _ => fun()}
    val start = System.nanoTime
    1.to(iterations).foreach { _ => fun()}
    val average = (System.nanoTime - start) / iterations.toFloat / 1000000f
    println(s"$msg: ${average}ms")
  }

  def newUUID(): String =
    java.util.UUID.randomUUID.toString.substring(0, 16)

}
