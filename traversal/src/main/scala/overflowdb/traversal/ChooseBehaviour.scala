package overflowdb.traversal

//import ChooseBehaviour.ComparisonStyle

import scala.annotation.tailrec
import scala.collection.mutable

trait ChooseBehaviour[TraversalElement, BranchOn, NewEnd] {
//  val comparisonStyle: ComparisonStyle.Value
}

object ChooseBehaviour {
  val Default: Nothing = ???

    class Builder[TraversalElement, BranchOn, NewEnd] {
//      private[this] var _comparisonStyle: ComparisonStyle.Value = ComparisonStyle.HashAndEquals

//      /* only compare the hashes when deduplicating elements. depending on the element type this can lead to
//      configure search algorithm to go "breadth first", rather than the default "depth first" */
//      def hashComparisonOnly: Builder = {
//        _comparisonStyle = ComparisonStyle.HashOnly
//        this
//      }

      // TODO limit X <: NewEnd or so?
      def withBranch[X](branchPf: PartialFunction[BranchOn, Traversal[TraversalElement] => Traversal[X]]): Builder[TraversalElement, BranchOn, X] = ???

      private[traversal] def build: ChooseBehaviour[TraversalElement, BranchOn, NewEnd] =
        new ChooseBehaviour[TraversalElement, BranchOn, NewEnd] {
//          override val comparisonStyle = _comparisonStyle
        }
    }
}

