/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2003-2013, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */


package scala.collection
package mutable

import scala.collection.generic._

/** This class implements priority queues using a heap.
  * To prioritize elements of type A there must be an implicit
  * Ordering[A] available at creation.
  *
  * @tparam A    type of the elements in this priority queue.
  * @param ord   implicit ordering used to compare the elements of type `A`.
  *
  * @author  Matthias Zenger
  * @version 1.0, 03/05/2004
  * @since   1
  *
  * @define Coll PrioritySet
  * @define coll priority queue
  * @define orderDependent
  * @define orderDependentFold
  * @define mayNotTerminateInf
  * @define willNotTerminateInf
  */
class PrioritySet[A](implicit val ord: Ordering[A])
    extends AbstractIterable[A]
            with Iterable[A]
            with GenericOrderedTraversableTemplate[A, PrioritySet]
            with IterableLike[A, PrioritySet[A]]
            with Growable[A]
            with Builder[A, PrioritySet[A]]
            with Serializable
            with scala.Cloneable {

  import ord._

  protected val resarr = new ResizableArrayAccess[A]

  override def size: Int = length

  def length: Int = resarr.length - 1 // adjust length accordingly

  resarr.p_size0 += 1

  // we do not use array(0)

  override def isEmpty: Boolean = resarr.p_size0 < 2

  override def repr = this

  def result = this

  override def orderedCompanion = PrioritySet

  /** Adds all elements provided by a `TraversableOnce` object
    * into the priority queue.
    *
    * @param  xs    a traversable object.
    * @return       a new priority queue containing elements of both `xs` and `this`.
    */
  def ++(xs: GenTraversableOnce[A]): PrioritySet[A] = {
    this.clone() ++= xs.seq
  }

  /** This method clones the priority queue.
    *
    * @return  a priority queue with the same elements.
    */
  override def clone(): PrioritySet[A] = new PrioritySet[A] ++= this.iterator

  /** Adds all elements to the queue.
    *
    * @param  elems       the elements to add.
    */
  def enqueue(elems: A*): Unit = {
    this ++= elems
  }

  def dequeueAll[A1 >: A, That](implicit bf: CanBuildFrom[_, A1, That]): That = {
    val b = bf.apply
    while (nonEmpty) {
      b += dequeue()
    }
    b.result
  }

  /** Returns the element with the highest priority in the queue,
    * and removes this element from the queue.
    *
    * @throws Predef.NoSuchElementException
    * @return   the element with the highest priority.
    */
  def dequeue(): A =
    if (resarr.p_size0 > 1) {
      resarr.p_size0 = resarr.p_size0 - 1
      resarr.p_swap(1, resarr.p_size0)
      fixDown(resarr.p_array, 1, resarr.p_size0 - 1)
      toA(resarr.p_array(resarr.p_size0))
    }
    else {
      throw new NoSuchElementException("no element to remove from heap")
    }

  /** Returns the element with the highest priority in the queue,
    * or throws an error if there is no element contained in the queue.
    *
    * @return   the element with the highest priority.
    */
  @deprecated("Use `head` instead.", "2.9.0")
  def max: A = if (resarr.p_size0 > 1) {
    toA(resarr.p_array(1))
  }
  else {
    throw new NoSuchElementException("queue is empty")
  }

  /** Returns the element with the highest priority in the queue,
    * or throws an error if there is no element contained in the queue.
    *
    * @return   the element with the highest priority.
    */
  override def head: A = if (resarr.p_size0 > 1) {
    toA(resarr.p_array(1))
  }
  else {
    throw new NoSuchElementException("queue is empty")
  }

  /** Removes all elements from the queue. After this operation is completed,
    * the queue will be empty.
    */
  def clear(): Unit = {
    resarr.p_size0 = 1
  }

  /** Returns the reverse of this queue. The priority queue that gets
    * returned will have an inversed ordering - if for some elements
    * `x` and `y` the original queue's ordering
    * had `compare` returning an integer ''w'', the new one will return ''-w'',
    * assuming the original ordering abides its contract.
    *
    * Note that the order of the elements will be reversed unless the
    * `compare` method returns 0. In this case, such elements
    * will be subsequent, but their corresponding subinterval may be inappropriately
    * reversed. However, due to the compare-equals contract, they will also be equal.
    *
    * @return   A reversed priority queue.
    */
  def reverse = {
    val revq = new PrioritySet[A]()(new scala.math.Ordering[A] {
      def compare(x: A, y: A) = ord.compare(y, x)
    })
    for (i <- 1 until resarr.length) {
      revq += resarr(i)
    }
    revq
  }

  /** Inserts a single element into the priority queue.
    *
    * @param  elem        the element to insert.
    * @return             this $coll.
    */
  def +=(elem: A): this.type = {
    // remove the old element if it exists
    val ndx = resarr.p_array.indexOf(elem.asInstanceOf[AnyRef])
    if (ndx >= 0) {
      resarr.p_swap(ndx, resarr.p_size0 - 1)
      resarr.p_size0 = resarr.p_size0 - 1
      fixDown(resarr.p_array, 1, resarr.p_size0 - 1)
    }

    resarr.p_ensureSize(resarr.p_size0 + 1)
    resarr.p_array(resarr.p_size0) = elem.asInstanceOf[AnyRef]
    fixUp(resarr.p_array, resarr.p_size0)
    resarr.p_size0 += 1

    this
  }

  protected def fixUp(as: Array[AnyRef], m: Int): Unit = {
    var k: Int = m
    while (k > 1 && toA(as(k / 2)) < toA(as(k))) {
      resarr.p_swap(k, k / 2)
      k = k / 2
    }
  }

  protected def fixDown(as: Array[AnyRef], m: Int, n: Int): Unit = {
    var k: Int = m
    while (n >= 2 * k) {
      var j = 2 * k
      if (j < n && toA(as(j)) < toA(as(j + 1))) {
        j += 1
      }
      if (toA(as(k)) >= toA(as(j))) {
        return
      }
      else {
        val h = as(k)
        as(k) = as(j)
        as(j) = h
        k = j
      }
    }
  }

  private def toA(x: AnyRef): A = x.asInstanceOf[A]

  /** Returns an iterator which yields all the elements in the reverse order
    * than that returned by the method `iterator`.
    *
    * Note: The order of elements returned is undefined.
    *
    * @return  an iterator over all elements sorted in descending order.
    */
  def reverseIterator: Iterator[A] = new AbstractIterator[A] {
    private var i = resarr.p_size0 - 1

    def hasNext: Boolean = i >= 1

    def next(): A = {
      val n = resarr.p_array(i)
      i -= 1
      toA(n)
    }
  }

  /** The hashCode method always yields an error, since it is not
    * safe to use mutable queues as keys in hash tables.
    *
    * @return never.
    */
  override def hashCode(): Int =
    throw new UnsupportedOperationException("unsuitable as hash key")

  /** Returns a regular queue containing the same elements.
    *
    * Note: the order of elements is undefined.
    */
  def toQueue: Queue[A] = new Queue[A] ++= this.iterator

  /** Returns a textual representation of a queue as a string.
    *
    * @return the string representation of this queue.
    */
  override def toString() = toList.mkString("PrioritySet(", ", ", ")")

  /** Converts this $coll to a list.
    *
    * Note: the order of elements is undefined.
    *
    * @return a list containing all elements of this $coll.
    */
  override def toList = this.iterator.toList

  /** Returns an iterator which yields all the elements.
    *
    * Note: The order of elements returned is undefined.
    * If you want to traverse the elements in priority queue
    * order, use `clone().dequeueAll.iterator`.
    *
    * @return  an iterator over all the elements.
    */
  override def iterator: Iterator[A] = new AbstractIterator[A] {
    private var i = 1

    def hasNext: Boolean = i < resarr.p_size0

    def next(): A = {
      val n = resarr.p_array(i)
      i += 1
      toA(n)
    }
  }

  protected[this] override def newBuilder = new PrioritySet[A]

  protected def contains(elem: A): Boolean = {
    resarr.p_array.indexOf(elem.asInstanceOf[AnyRef]) >= 0
  }

  def find(elem: A): A = {
    val ndx = resarr.p_array.indexOf(elem.asInstanceOf[AnyRef])
    if (ndx >= 0) {
      return resarr.p_array(ndx).asInstanceOf[A]
    }
    throw new NoSuchElementException("no element to find")
  }

  protected def remove(elem: A): Unit = {
    val ndx = resarr.p_array.indexOf(elem.asInstanceOf[AnyRef])
    if (ndx >= 0) {
      resarr.p_swap(ndx, resarr.p_size0 - 1)
      resarr.p_size0 = resarr.p_size0 - 1
      fixDown(resarr.p_array, 1, resarr.p_size0 - 1)
    }
  }

  protected class ResizableArrayAccess[A] extends AbstractSeq[A] with ResizableArray[A] {
    def p_size0 = size0

    def p_size0_=(s: Int) = size0 = s

    def p_array = array

    def p_ensureSize(n: Int) = super.ensureSize(n)

    def p_swap(a: Int, b: Int) = super.swap(a, b)
  }

  // def printstate() {
  //   println("-----------------------")
  //   println("Size: " + resarr.p_size0)
  //   println("Internal array: " + resarr.p_array.toList)
  //   println(toString)
  // }
}


object PrioritySet extends OrderedTraversableFactory[PrioritySet] {
  def newBuilder[A](implicit ord: Ordering[A]) = new PrioritySet[A]

  implicit def canBuildFrom[A](implicit
      ord: Ordering[A]): CanBuildFrom[Coll, A, PrioritySet[A]] = new GenericCanBuildFrom[A]
}

