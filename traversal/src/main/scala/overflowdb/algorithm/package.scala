package overflowdb

package object algorithm {

  trait GetParents[A] {
    def apply(a: A): Set[A]
  }

}
