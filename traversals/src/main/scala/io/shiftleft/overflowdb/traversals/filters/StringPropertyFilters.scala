package io.shiftleft.overflowdb.traversals.filters

import io.shiftleft.overflowdb.traversals.Traversal

object StringPropertyFilters {
  def regexp[NodeType](trav: Traversal[NodeType])(accessor: NodeType => String, regexp: String): Traversal[NodeType] = {
    val valueRegexp = regexp.r
    trav.filter(node => valueRegexp.matches(accessor(node)))
  }

  def regexpNot[NodeType](trav: Traversal[NodeType])(accessor: NodeType => String, regexp: String): Traversal[NodeType] = {
    val valueRegexp = regexp.r
    trav.filter(node => !valueRegexp.matches(accessor(node)))
  }

  def regexpMultiple[NodeType](trav: Traversal[NodeType])(accessor: NodeType => String, regexps: Seq[String]): Traversal[NodeType] = {
    val valueRegexps = regexps.map(_.r)
    trav.filter { node =>
      val value = accessor(node)
      valueRegexps.find(_.matches(value)).isDefined
    }
  }

  def regexpNotMultiple[NodeType](trav: Traversal[NodeType])(accessor: NodeType => String, regexps: Seq[String]): Traversal[NodeType] = {
    val valueRegexps = regexps.map(_.r)
    trav.filter { node =>
      val value = accessor(node)
      valueRegexps.find(_.matches(value)).isEmpty
    }
  }

  def contains[NodeType](trav: Traversal[NodeType])(accessor: NodeType => String, value: String): Traversal[NodeType] =
    trav.filter(accessor(_).contains(value))

  def containsNot[NodeType](trav: Traversal[NodeType])(accessor: NodeType => String, value: String): Traversal[NodeType] =
    trav.filterNot(accessor(_).contains(value))

  def startsWith[NodeType](trav: Traversal[NodeType])(accessor: NodeType => String, value: String): Traversal[NodeType] =
    trav.filter(accessor(_).startsWith(value))

  def endsWith[NodeType](trav: Traversal[NodeType])(accessor: NodeType => String, value: String): Traversal[NodeType] =
    trav.filter(accessor(_).endsWith(value))
}