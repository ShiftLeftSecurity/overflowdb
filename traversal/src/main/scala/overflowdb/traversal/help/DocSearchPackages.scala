package overflowdb.traversal.help

/** defines where we should search for @Traversal/@TraversalSource/@Doc annotations
  */
trait DocSearchPackages {
  def apply(): Seq[String]
}

object DocSearchPackages {

  /** default implicit, for domains that don't have custom steps: no additional packages to search */
  implicit val defaultDocSearchPackage: DocSearchPackages = () => Nil

  def apply(searchPackages: String*): DocSearchPackages =
    () => searchPackages
}
