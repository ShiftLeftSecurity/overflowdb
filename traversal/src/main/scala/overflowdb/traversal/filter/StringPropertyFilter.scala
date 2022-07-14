package overflowdb.traversal.filter

import overflowdb.traversal.Traversal

import java.util.regex.PatternSyntaxException
import scala.collection.mutable
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

  /** compiles given string into a Regex which can be reused
    * prefixes given string with `(?s)` to enable multi line matching
    */
  def regexpCompile(regexp: String): Regex =
    try {
      s"(?s)$regexp".r
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

  def exactMultiple[NodeType, ValueType](traversal: Traversal[NodeType],
                                         accessor: NodeType => Option[ValueType],
                                         needles: Seq[ValueType],
                                         indexName: String): Traversal[NodeType] = {
    if (needles.isEmpty)
      return Traversal.empty

    traversal match {
      case init: overflowdb.traversal.InitialTraversal[NodeType] if init.canUseIndex(indexName) =>
        needles
          .to(Traversal)
          .flatMap(needle => init.getByIndex(indexName, needle).get)
      case _ =>
        var iteration = 0
        var needleSet: mutable.HashSet[ValueType] = null

        traversal.filter { node =>
          iteration += 1

          accessor(node).exists { value =>
            // Creating and accessing HashSets is expensive, so just sequentially scan needles for the first iteration.
            // (A max of 1 iteration happens regularly with .where/.whereNot clauses.)
            if (iteration >= 2 && needleSet == null)
              needleSet = needles.to(mutable.HashSet)

            if (needleSet == null) needles.contains(value)
            else needleSet.contains(value)
          }
        }
    }
  }

}
