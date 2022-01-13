package overflowdb.traversal.help

trait DocSearchPackages {
  def apply(): Seq[String]
}

object DocSearchPackages {
  /** default implicit, for domains that don't have custom steps: no additional packages to search */
  implicit val defaultDocSearchPackage: DocSearchPackages = () => Nil
}

