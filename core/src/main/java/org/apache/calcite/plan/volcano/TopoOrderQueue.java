package org.apache.calcite.plan.volcano;

import java.util.*;

public class TopoOrderQueue<E> extends PriorityQueue<E> {
  private final Set<E> set = new HashSet<>();

  public TopoOrderQueue(Comparator<E> comparator) {
    super(comparator);
  }

  @Override
  public boolean offer(E e) {
    if (set.add(e)) {
      return super.offer(e);
    }

    modCount++;
    int k = indexOf(e);
    while (k > 0) {
      int parent = (k - 1) >>> 1;
      queue[k] = queue[parent];
      k = parent;
    }
    queue[0] = e;
    siftDown(0, e);

    return false;
  }

  @Override
  public E poll() {
    E e = super.poll();
    set.remove(e);
    return e;
  }

  @Override
  public void clear() {
    super.clear();
    set.clear();
  }

  /**
   * Value of a (possibly) existing item has changed, re-adjust its position.
   */
  public void heapify(E e) {
    if (!set.contains(e)) {
      return;
    }

    modCount++;
    int k = indexOf(e);
    while (k > 0) {
      int parent = (k - 1) >>> 1;
      queue[k] = queue[parent];
      k = parent;
    }
    queue[0] = e;
    siftDown(0, e);
  }
}

class PriorityQueue<E> {

  private static final int DEFAULT_INITIAL_CAPACITY = 11;

  /**
   * Priority queue represented as a balanced binary heap: the two
   * children of queue[n] are queue[2*n+1] and queue[2*(n+1)].  The
   * priority queue is ordered by comparator, or by the elements'
   * natural ordering, if comparator is null: For each node n in the
   * heap and each descendant d of n, n <= d.  The element with the
   * lowest value is in queue[0], assuming the queue is nonempty.
   */
  transient Object[] queue; // non-private to simplify nested class access

  /**
   * The number of elements in the priority queue.
   */
  protected int size = 0;

  /**
   * The comparator, or null if priority queue uses elements'
   * natural ordering.
   */
  private final Comparator<? super E> comparator;

  /**
   * The number of times this priority queue has been
   * <i>structurally modified</i>.  See AbstractList for gory details.
   */
  transient int modCount = 0; // non-private to simplify nested class access

  /**
   * Creates a {@code PriorityQueue} with the default initial capacity and
   * whose elements are ordered according to the specified comparator.
   *
   * @param  comparator the comparator that will be used to order this
   *         priority queue.  If {@code null}, the {@linkplain Comparable
   *         natural ordering} of the elements will be used.
   * @since 1.8
   */
  public PriorityQueue(Comparator<? super E> comparator) {
    this(DEFAULT_INITIAL_CAPACITY, comparator);
  }

  /**
   * Creates a {@code PriorityQueue} with the specified initial capacity
   * that orders its elements according to the specified comparator.
   *
   * @param  initialCapacity the initial capacity for this priority queue
   * @param  comparator the comparator that will be used to order this
   *         priority queue.  If {@code null}, the {@linkplain Comparable
   *         natural ordering} of the elements will be used.
   * @throws IllegalArgumentException if {@code initialCapacity} is
   *         less than 1
   */
  public PriorityQueue(int initialCapacity, Comparator<? super E> comparator) {
    // Note: This restriction of at least one is not actually needed,
    // but continues for 1.5 compatibility
    if (initialCapacity < 1)
      throw new IllegalArgumentException();
    this.queue = new Object[initialCapacity];
    this.comparator = comparator;
  }

  /**
   * The maximum size of array to allocate.
   * Some VMs reserve some header words in an array.
   * Attempts to allocate larger arrays may result in
   * OutOfMemoryError: Requested array size exceeds VM limit
   */
  private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

  /**
   * Increases the capacity of the array.
   *
   * @param minCapacity the desired minimum capacity
   */
  private void grow(int minCapacity) {
    int oldCapacity = queue.length;
    // Double size if small; else grow by 50%
    int newCapacity = oldCapacity + ((oldCapacity < 64) ?
        (oldCapacity + 2) :
        (oldCapacity >> 1));
    // overflow-conscious code
    if (newCapacity - MAX_ARRAY_SIZE > 0)
      newCapacity = hugeCapacity(minCapacity);
    queue = Arrays.copyOf(queue, newCapacity);
  }

  private static int hugeCapacity(int minCapacity) {
    if (minCapacity < 0) // overflow
      throw new OutOfMemoryError();
    return (minCapacity > MAX_ARRAY_SIZE) ?
        Integer.MAX_VALUE :
        MAX_ARRAY_SIZE;
  }

  /**
   * Inserts the specified element into this priority queue.
   *
   * @return {@code true} (as specified by {@link Collection#add})
   * @throws ClassCastException if the specified element cannot be
   *         compared with elements currently in this priority queue
   *         according to the priority queue's ordering
   * @throws NullPointerException if the specified element is null
   */
  public boolean add(E e) {
    return offer(e);
  }

  /**
   * Adds all of the elements in the specified collection to this
   * queue.  Attempts to addAll of a queue to itself result in
   * <tt>IllegalArgumentException</tt>. Further, the behavior of
   * this operation is undefined if the specified collection is
   * modified while the operation is in progress.
   *
   * <p>This implementation iterates over the specified collection,
   * and adds each element returned by the iterator to this
   * queue, in turn.  A runtime exception encountered while
   * trying to add an element (including, in particular, a
   * <tt>null</tt> element) may result in only some of the elements
   * having been successfully added when the associated exception is
   * thrown.
   *
   * @param c collection containing elements to be added to this queue
   * @return <tt>true</tt> if this queue changed as a result of the call
   * @throws ClassCastException if the class of an element of the specified
   *         collection prevents it from being added to this queue
   * @throws NullPointerException if the specified collection contains a
   *         null element and this queue does not permit null elements,
   *         or if the specified collection is null
   * @throws IllegalArgumentException if some property of an element of the
   *         specified collection prevents it from being added to this
   *         queue, or if the specified collection is this queue
   * @throws IllegalStateException if not all the elements can be added at
   *         this time due to insertion restrictions
   * @see #add(Object)
   */
  public boolean addAll(Collection<? extends E> c) {
    if (c == null)
      throw new NullPointerException();
    if (c == this)
      throw new IllegalArgumentException();
    boolean modified = false;
    for (E e : c)
      if (add(e))
        modified = true;
    return modified;
  }

  /**
   * Inserts the specified element into this priority queue.
   *
   * @return {@code true} (as specified by {@link Queue#offer})
   * @throws ClassCastException if the specified element cannot be
   *         compared with elements currently in this priority queue
   *         according to the priority queue's ordering
   * @throws NullPointerException if the specified element is null
   */
  public boolean offer(E e) {
    if (e == null)
      throw new NullPointerException();
    modCount++;
    int i = size;
    if (i >= queue.length)
      grow(i + 1);
    size = i + 1;
    if (i == 0)
      queue[0] = e;
    else
      siftUp(i, e);
    return true;
  }

  @SuppressWarnings("unchecked")
  public E peek() {
    return (size == 0) ? null : (E) queue[0];
  }

  protected int indexOf(Object o) {
    if (o != null) {
      for (int i = 0; i < size; i++)
        if (o.equals(queue[i]))
          return i;
    }
    return -1;
  }

  /**
   * Returns {@code true} if this queue contains the specified element.
   * More formally, returns {@code true} if and only if this queue contains
   * at least one element {@code e} such that {@code o.equals(e)}.
   *
   * @param o object to be checked for containment in this queue
   * @return {@code true} if this queue contains the specified element
   */
  public boolean contains(Object o) {
    return indexOf(o) != -1;
  }

  /**
   * Returns an array containing all of the elements in this queue.
   * The elements are in no particular order.
   *
   * <p>The returned array will be "safe" in that no references to it are
   * maintained by this queue.  (In other words, this method must allocate
   * a new array).  The caller is thus free to modify the returned array.
   *
   * <p>This method acts as bridge between array-based and collection-based
   * APIs.
   *
   * @return an array containing all of the elements in this queue
   */
  public Object[] toArray() {
    return Arrays.copyOf(queue, size);
  }

  /**
   * Returns an array containing all of the elements in this queue; the
   * runtime type of the returned array is that of the specified array.
   * The returned array elements are in no particular order.
   * If the queue fits in the specified array, it is returned therein.
   * Otherwise, a new array is allocated with the runtime type of the
   * specified array and the size of this queue.
   *
   * <p>If the queue fits in the specified array with room to spare
   * (i.e., the array has more elements than the queue), the element in
   * the array immediately following the end of the collection is set to
   * {@code null}.
   *
   * <p>Like the {@link #toArray()} method, this method acts as bridge between
   * array-based and collection-based APIs.  Further, this method allows
   * precise control over the runtime type of the output array, and may,
   * under certain circumstances, be used to save allocation costs.
   *
   * <p>Suppose {@code x} is a queue known to contain only strings.
   * The following code can be used to dump the queue into a newly
   * allocated array of {@code String}:
   *
   *  <pre> {@code String[] y = x.toArray(new String[0]);}</pre>
   *
   * Note that {@code toArray(new Object[0])} is identical in function to
   * {@code toArray()}.
   *
   * @param a the array into which the elements of the queue are to
   *          be stored, if it is big enough; otherwise, a new array of the
   *          same runtime type is allocated for this purpose.
   * @return an array containing all of the elements in this queue
   * @throws ArrayStoreException if the runtime type of the specified array
   *         is not a supertype of the runtime type of every element in
   *         this queue
   * @throws NullPointerException if the specified array is null
   */
  @SuppressWarnings("unchecked")
  public <T> T[] toArray(T[] a) {
    final int size = this.size;
    if (a.length < size)
      // Make a new array of a's runtime type, but my contents:
      return (T[]) Arrays.copyOf(queue, size, a.getClass());
    System.arraycopy(queue, 0, a, 0, size);
    if (a.length > size)
      a[size] = null;
    return a;
  }

  /**
   * <p>This implementation returns <tt>size() == 0</tt>.
   */
  public boolean isEmpty() {
    return size() == 0;
  }

  /**
   * Returns an iterator over the elements in this queue. The iterator
   * does not return the elements in any particular order.
   *
   * @return an iterator over the elements in this queue
   */
  public Iterator<E> iterator() {
    return new Itr();
  }

  private final class Itr implements Iterator<E> {
    /**
     * Index (into queue array) of element to be returned by
     * subsequent call to next.
     */
    private int cursor = 0;

    /**
     * The modCount value that the iterator believes that the backing
     * Queue should have.  If this expectation is violated, the iterator
     * has detected concurrent modification.
     */
    private int expectedModCount = modCount;

    public boolean hasNext() {
      return cursor < size;
    }

    @SuppressWarnings("unchecked")
    public E next() {
      if (expectedModCount != modCount)
        throw new ConcurrentModificationException();
      if (cursor < size)
        return (E) queue[cursor++];
      throw new NoSuchElementException();
    }
  }

  public int size() {
    return size;
  }

  /**
   * Removes all of the elements from this priority queue.
   * The queue will be empty after this call returns.
   */
  public void clear() {
    modCount++;
    for (int i = 0; i < size; i++)
      queue[i] = null;
    size = 0;
  }

  @SuppressWarnings("unchecked")
  public E poll() {
    if (size == 0)
      return null;
    int s = --size;
    modCount++;
    E result = (E) queue[0];
    E x = (E) queue[s];
    queue[s] = null;
    if (s != 0)
      siftDown(0, x);
    return result;
  }

  /**
   * Inserts item x at position k, maintaining heap invariant by
   * promoting x up the tree until it is greater than or equal to
   * its parent, or is the root.
   *
   * To simplify and speed up coercions and comparisons. the
   * Comparable and Comparator versions are separated into different
   * methods that are otherwise identical. (Similarly for siftDown.)
   *
   * @param k the position to fill
   * @param x the item to insert
   */
  @SuppressWarnings("unchecked")
  protected void siftUp(int k, E x) {
    while (k > 0) {
      int parent = (k - 1) >>> 1;
      Object e = queue[parent];
      if (comparator.compare(x, (E) e) >= 0)
        break;
      queue[k] = e;
      k = parent;
    }
    queue[k] = x;
  }

  /**
   * Inserts item x at position k, maintaining heap invariant by
   * demoting x down the tree repeatedly until it is less than or
   * equal to its children or is a leaf.
   *
   * @param k the position to fill
   * @param x the item to insert
   */
  @SuppressWarnings("unchecked")
  protected void siftDown(int k, E x) {
    int half = size >>> 1;
    while (k < half) {
      int child = (k << 1) + 1;
      Object c = queue[child];
      int right = child + 1;
      if (right < size &&
          comparator.compare((E) c, (E) queue[right]) > 0)
        c = queue[child = right];
      if (comparator.compare(x, (E) c) <= 0)
        break;
      queue[k] = c;
      k = child;
    }
    queue[k] = x;
  }

  /**
   * Returns the comparator used to order the elements in this
   * queue, or {@code null} if this queue is sorted according to
   * the {@linkplain Comparable natural ordering} of its elements.
   *
   * @return the comparator used to order this queue, or
   *         {@code null} if this queue is sorted according to the
   *         natural ordering of its elements
   */
  public Comparator<? super E> comparator() {
    return comparator;
  }
}