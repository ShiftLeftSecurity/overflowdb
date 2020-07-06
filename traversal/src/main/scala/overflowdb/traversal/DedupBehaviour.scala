package overflowdb.traversal

import DedupBehaviour.ComparisonStyle

import scala.annotation.tailrec
import scala.collection.mutable

trait DedupBehaviour {
  val comparisonStyle: ComparisonStyle.Value
}

object DedupBehaviour {
    object ComparisonStyle extends Enumeration {
      type ComparisonStyle = Value
      val HashAndEquals, HashOnly = Value
    }

    def noop(builder: DedupBehaviour.Builder): Builder = builder

    class Builder {
      private[this] var _comparisonStyle: ComparisonStyle.Value = ComparisonStyle.HashAndEquals

      /* only compare the hashes when deduplicating elements. depending on the element type this can lead to
      configure search algorithm to go "breadth first", rather than the default "depth first" */
      def hashComparisonOnly: Builder = {
        _comparisonStyle = ComparisonStyle.HashOnly
        this
      }

      private[traversal] def build: DedupBehaviour =
        new DedupBehaviour {
          override val comparisonStyle = _comparisonStyle
        }
    }
}

class DedupByHashIterator[A](elements: IterableOnce[A]) extends Iterator[A] {
  private var _next: A = _
  private var _nextLoaded = false
  private val hashesOfSeenElements = mutable.Set.empty[Int]

  @tailrec
  final override def hasNext: Boolean = {
    if (_nextLoaded) {
      true
    } else if (elements.isEmpty) {
      false
    } else {
      val nextElement = elements.next
      val nextElementHash = nextElement.hashCode
      if (hashesOfSeenElements.contains(nextElementHash)) {
        hasNext
      } else {
        _next = nextElement
        _nextLoaded = true
        hashesOfSeenElements.add(nextElementHash)
        true
      }
    }
  }

  override def next(): A = {
    if (hasNext) {
      _nextLoaded = false
      _next
    } else {
      throw new NoSuchElementException("next on empty iterator")
    }
  }
}
