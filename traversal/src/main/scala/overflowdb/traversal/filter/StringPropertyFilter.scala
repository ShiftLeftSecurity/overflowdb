package overflowdb.traversal.filter

import java.util.regex.PatternSyntaxException
import overflowdb.traversal.Traversal
import scala.util.matching.Regex

object StringPropertyFilter {

  def regexp[NodeType](trav: Traversal[NodeType])(accessor: NodeType => String,
                                                  regexp: String): Traversal[NodeType] = {
    val valueRegex = regexpCompile(regexp)
    trav.filter(node => valueRegex.matches(accessor(node)))
  }

  def regexpNot[NodeType](trav: Traversal[NodeType])(accessor: NodeType => String,
                                                     regexp: String): Traversal[NodeType] = {
    val valueRegex = regexpCompile(regexp)
    trav.filter(node => !valueRegex.matches(accessor(node)))
  }

  def regexpMultiple[NodeType](trav: Traversal[NodeType])(
      accessor: NodeType => String,
      regexps: Seq[String]): Traversal[NodeType] = {
    val valueRegexs = regexps.map(regexpCompile)
    trav.filter { node =>
      val value = accessor(node)
      valueRegexs.find(_.matches(value)).isDefined
    }
  }

  def regexpNotMultiple[NodeType](trav: Traversal[NodeType])(
      accessor: NodeType => String,
      regexps: Seq[String]): Traversal[NodeType] = {
    val valueRegexs = regexps.map(regexpCompile)
    trav.filter { node =>
      val value = accessor(node)
      valueRegexs.find(_.matches(value)).isEmpty
    }
  }

  private def regexpCompile(regexp: String): Regex =
    try {
      regexp.r
    } catch {
      case e: PatternSyntaxException =>
        throw new InvalidRegexException(regexp, e)
    }
  class InvalidRegexException(regexp: String, cause: PatternSyntaxException)
      extends RuntimeException(s"invalid regular expression: `$regexp`", cause)

  def contains[NodeType](trav: Traversal[NodeType])(accessor: NodeType => String,
                                                    value: String): Traversal[NodeType] =
    trav.filter(accessor(_).contains(value))

  def containsNot[NodeType](trav: Traversal[NodeType])(accessor: NodeType => String,
                                                       value: String): Traversal[NodeType] =
    trav.filterNot(accessor(_).contains(value))

  def startsWith[NodeType](trav: Traversal[NodeType])(accessor: NodeType => String,
                                                      value: String): Traversal[NodeType] =
    trav.filter(accessor(_).startsWith(value))

  def endsWith[NodeType](trav: Traversal[NodeType])(accessor: NodeType => String,
                                                    value: String): Traversal[NodeType] =
    trav.filter(accessor(_).endsWith(value))

}
