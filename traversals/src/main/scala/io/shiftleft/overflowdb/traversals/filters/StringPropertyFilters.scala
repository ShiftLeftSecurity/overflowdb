package io.shiftleft.overflowdb.traversals.filters

import io.shiftleft.overflowdb.traversals.Traversal

object StringPropertyFilters extends PropertyFilters {
  def filterRegexp[NodeType](trav: Traversal[NodeType])(accessor: NodeType => String, regexp: String): Traversal[NodeType] = {
    val valueRegexp = regexp.r
    trav.filter(node => valueRegexp.matches(accessor(node)))
  }

  def filterNotRegexp[NodeType](trav: Traversal[NodeType])(accessor: NodeType => String, regexp: String): Traversal[NodeType] = {
    val valueRegexp = regexp.r
    trav.filter(node => !valueRegexp.matches(accessor(node)))
  }

  def filterRegexpMultiple[NodeType](trav: Traversal[NodeType])(accessor: NodeType => String, regexps: Seq[String]): Traversal[NodeType] = {
    val valueRegexps = regexps.map(_.r)
    trav.filter { node =>
      val value = accessor(node)
      valueRegexps.find(_.matches(value)).isDefined
    }
  }

  def filterNotRegexpMultiple[NodeType](trav: Traversal[NodeType])(accessor: NodeType => String, regexps: Seq[String]): Traversal[NodeType] = {
    val valueRegexps = regexps.map(_.r)
    trav.filter { node =>
      val value = accessor(node)
      valueRegexps.find(_.matches(value)).isEmpty
    }
  }
}