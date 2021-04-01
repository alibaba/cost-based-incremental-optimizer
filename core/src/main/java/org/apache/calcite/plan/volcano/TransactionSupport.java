package org.apache.calcite.plan.volcano;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

interface TransactionSupport {
  void rollback();
  void commit();
}

/**
 * A Set that supports TransactionSupport.
 */
class TxnSet<E> implements Set<E>, TransactionSupport {

  // committed and staging never share the same item,
  // this allows easy implementations for iterator(), size(), etc.
  private Set<E> committed;

  // Added items that are not committed yet
  private Set<E> staging;

  // Items already removed from "committed" that's not committed yet,
  // committed and removedFromCommitted also never share the same item
  private Set<E> removedFromCommitted;

  TxnSet(Set<E> committed, Set<E> staging, Set<E> removedFromCommitted) {
    this.committed = committed;
    this.staging = staging;
    this.removedFromCommitted = removedFromCommitted;
  }

  @Override
  public void rollback() {
    int n = committed.size() + removedFromCommitted.size();
    committed.addAll(removedFromCommitted);
    assert committed.size() == n : "Duplicate item detected";
    removedFromCommitted.clear();
    staging.clear();
  }

  @Override
  public void commit() {
    if (committed.isEmpty()) {
      Set<E> tmp = committed;
      committed = staging;
      staging = tmp;
    } else {
      int n = committed.size() + staging.size();
      committed.addAll(staging);
      assert committed.size() == n : "Duplicate item detected";
      staging.clear();
    }
    removedFromCommitted.clear();
  }

  @Override
  public int size() {
    return committed.size() + staging.size();
  }

  @Override
  public boolean isEmpty() {
    return committed.isEmpty() && staging.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return committed.contains(o) || staging.contains(o);
  }

  @Override
  public boolean add(E e) {
    if (contains(e)) {
      return false;
    }
    return staging.add(e);
  }

  @Override
  public boolean remove(Object o) {
    if (!contains(o)) {
      return false;
    }
    if (staging.remove(o)) {
      return true;
    }
    committed.remove(o);
    boolean tmp = removedFromCommitted.add((E) o);
    assert tmp : "Duplicate item detected";
    return true;
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return c.stream().allMatch(this::contains);
  }

  @Override
  public boolean addAll(Collection<? extends E> c) {
    return c.stream().map(this::add).reduce(false, (x, y) -> x || y);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return c.stream().map(this::remove).reduce(false, (x, y) -> x || y);
  }

  @Override
  public void clear() {
    staging.clear();
    if (removedFromCommitted.isEmpty()) {
      Set<E> tmp = committed;
      committed = removedFromCommitted;
      removedFromCommitted = tmp;
    } else {
      int n = committed.size() + removedFromCommitted.size();
      removedFromCommitted.addAll(committed);
      assert removedFromCommitted.size() == n : "Duplicate item detected";
      committed.clear();
    }
  }

  @Override
  public Iterator<E> iterator() {
    // Force all modifications to use add() and remove()
    return Iterators.unmodifiableIterator(
        Iterators.concat(committed.iterator(), staging.iterator()));
  }

  @Override
  public Object[] toArray() {
    return ImmutableList.copyOf(iterator()).toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return ImmutableList.copyOf(iterator()).toArray(a);
  }
}

/**
 * A Map that supports TransactionSupport.
 */
class TxnMap<K, V> implements Map<K, V>, TransactionSupport {

  // committed and staging never share the same key,
  // this allows easy implementations for entrySet(), size(), etc.
  private Map<K, V> committed;

  // Added entries that are not committed yet
  private Map<K, V> staging;

  // Entries already removed from "committed" that's not committed yet,
  // committed and removedFromCommitted also never share the same key
  private Map<K, V> removedFromCommitted;

  TxnMap(Supplier<Map<K, V>> mapSupplier) {
    this.committed = mapSupplier.get();
    this.staging = mapSupplier.get();
    this.removedFromCommitted = mapSupplier.get();
  }

  @Override
  public void rollback() {
    int n = committed.size() + removedFromCommitted.size();
    committed.putAll(removedFromCommitted);
    assert committed.size() == n : "Duplicate key detected";
    removedFromCommitted.clear();
    staging.clear();
  }

  @Override
  public void commit() {
    if (committed.isEmpty()) {
      Map<K, V> tmp = committed;
      committed = staging;
      staging = tmp;
    } else {
      int n = committed.size() + staging.size();
      committed.putAll(staging);
      assert committed.size() == n : "Duplicate key detected";
      staging.clear();
    }
    removedFromCommitted.clear();
  }

  @Override
  public int size() {
    return committed.size() + staging.size();
  }

  public boolean isEmpty() {
    return committed.isEmpty() && staging.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return committed.containsKey(key) || staging.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return committed.containsValue(value) || staging.containsValue(value);
  }

  @Override
  public V get(Object key) {
    V ret = staging.get(key);
    return (ret != null) ? ret : committed.get(key);
  }

  @Override
  public V put(K key, V value) {
    V old = staging.put(key, value);
    if (old == null) {
      old = committed.remove(key);
      if (old != null) {
        V tmp = removedFromCommitted.put(key, old);
        assert tmp == null : "Duplicate key detected";
      }
    }
    return old;
  }

  @Override
  public V remove(Object key) {
    V value = staging.remove(key);
    if (value == null) {
      value = committed.remove(key);
      if (value != null) {
        V tmp = removedFromCommitted.put((K) key, value);
        assert tmp == null : "Duplicate key detected";
      }
    }
    return value;
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    m.forEach(this::put);
  }

  @Override
  public void clear() {
    staging.clear();
    if (removedFromCommitted.isEmpty()) {
      Map<K, V> tmp = committed;
      committed = removedFromCommitted;
      removedFromCommitted = tmp;
    } else {
      int n = committed.size() + removedFromCommitted.size();
      removedFromCommitted.putAll(committed);
      assert removedFromCommitted.size() == n : "Duplicate key detected";
      committed.clear();
    }
  }

  @Override
  public Set<K> keySet() {
    return new TxnSet<>(committed.keySet(), staging.keySet(),
        removedFromCommitted.keySet());
  }

  @Override
  public Collection<V> values() {
    // TODO: the returned list is the current snapshot of the values. It
    //  does NOT change with the underlying map.
    // Force all modifications to use put() and remove()
    return Collections.unmodifiableList(
        Stream.concat(committed.values().stream(), staging.values().stream())
            .collect(Collectors.toList()));
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    return new TxnSet<>(committed.entrySet(), staging.entrySet(),
        removedFromCommitted.entrySet());
  }
}
