package io.shiftleft.overflowdb.traversals.filters

import io.shiftleft.overflowdb.traversals.Traversal

object StringPropertyFilters extends PropertyFilters {
  def filterRegexp[Trav <: Traversal[NodeType], NodeType, Value](trav: Trav, accessor: NodeType => String, regexp: String): Traversal[NodeType] = {
    val valueRegexp = regexp.r
    trav.filter(node => valueRegexp.matches(accessor(node)))
  }

  def filterNotRegexp[Trav <: Traversal[NodeType], NodeType, Value](trav: Trav, accessor: NodeType => String, regexp: String): Traversal[NodeType] = {
    val valueRegexp = regexp.r
    trav.filter(node => !valueRegexp.matches(accessor(node)))
  }

  def filterRegexpMultiple[Trav <: Traversal[NodeType], NodeType, Value](trav: Trav, accessor: NodeType => String, regexps: Seq[String]): Traversal[NodeType] = {
    val valueRegexps = regexps.map(_.r)
    trav.filter { node =>
      val value = accessor(node)
      valueRegexps.find(_.matches(value)).isDefined
    }
  }

  def filterNotRegexpMultiple[Trav <: Traversal[NodeType], NodeType, Value](trav: Trav, accessor: NodeType => String, regexps: Seq[String]): Traversal[NodeType] = {
    val valueRegexps = regexps.map(_.r)
    trav.filter { node =>
      val value = accessor(node)
      valueRegexps.find(_.matches(value)).isEmpty
    }
  }
}