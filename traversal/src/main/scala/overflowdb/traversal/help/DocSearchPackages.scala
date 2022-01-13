package overflowdb.traversal.help

//case class DocSearchPackages(packages: Seq[String])

trait DocSearchPackages {
  def apply(): Seq[String]
}

object DocSearchPackages {
  implicit val defaultDocSearchPackage: DocSearchPackages =
    () => Seq("default.foo")

}

//trait LowPrioImplicits

