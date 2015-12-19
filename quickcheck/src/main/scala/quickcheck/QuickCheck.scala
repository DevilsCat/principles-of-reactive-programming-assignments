package quickcheck

import common._

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  // If you insert any element into an empty heap, finding
  // the minimum of the resulting heap should get that elements
  // back.
  property("min1") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }
  
  // If you insert any two elements into an empty heap, finding 
  // the minimum of the resulting heap should get the smallest 
  // of the two elements back.
  property("two-elements") = forAll { (x: Int, y: Int) =>
    val h = insert(y, insert(x, empty))
    findMin(h) == Math.min(x, y)
  }
  
  // If you insert an element into an empty heap, then delete the 
  // minimum, the resulting heap should be empty.
  property("delete1") = forAll { (a: Int) =>
    val h = insert(a, empty)
    isEmpty(deleteMin(h))
  }
  
  // If you insert three elements into an empty heap, then delete the
  // minimum, finding the minimum of the resulting heap should get the
  // median elements back.
  property("delete2") = forAll { (x: Int, y: Int, z: Int) =>
    val kMedianValIdx = 1
    val h = insert(x, (insert(y, insert(z, empty))))
    findMin(deleteMin(h)) == List(x, y, z).sorted.apply(kMedianValIdx)
  }
  
  // Given any heap, you should get a sorted sequence of 
  // elements when continually finding and deleting minima. 
  // (Hint: recursion and helper functions are your friends.)
  property("get-sorted-seq") = forAll { (h: H) =>
    def checkOrdering(prevElem: Int, h: H): Boolean = {
      if (isEmpty(h)) true 
      else
        if (prevElem > findMin(h)) false 
        else checkOrdering(findMin(h), deleteMin(h))
    }
    checkOrdering(Int.MinValue, h)
  }
  
  // Finding a minimum of the melding of any two heaps should 
  // return a minimum of one or the other.
  property("meld") = forAll { (h1: H, h2: H) =>
    findMin(meld(h1, h2)) == Math.min(findMin(h1), findMin(h2))
  }
  
  // Generator for arbitrary heap.
  lazy val genHeap: Gen[H] = for {
    elem <- arbitrary[A]
    heap <- oneOf[H](empty, genHeap)  // type cast on oneOf such that heap is an H not Any
  } yield insert(elem, heap)

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

}
