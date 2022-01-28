package helptest

import overflowdb.traversal.{Traversal, help}
import overflowdb.traversal.help.Doc
import overflowdb.traversal.testdomains.simple.Thing

/**
 * Example for domain specific extension steps that are defined in a different package.
 * TraversalTests verifies that the .help step finds the documentation as specified in @Doc
 *
 * @param traversal
 */
@help.Traversal(elementType = classOf[Thing])
class SimpleDomainTraversal(val traversal: Traversal[Thing]) extends AnyVal {

  @Doc(info = "name2 (just like name, but in a different package...)")
  def name2: Traversal[String] = traversal.map(_.name)

}
