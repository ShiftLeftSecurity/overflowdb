package helptest

import overflowdb.traversal.{Traversal, help}
import overflowdb.traversal.help.Doc
import overflowdb.traversal.testdomains.simple.Thing

@help.Traversal(elementType = classOf[Thing])
class SimpleDomainTraversal(val traversal: Traversal[Thing]) extends AnyVal {

  @Doc(info = "name2",
       longInfo = "name2 (just like name, but in a different package...)",
       example = "SimpleDomain.thing.name2")
  def name2: Traversal[String] = traversal.map(_.name)

}
