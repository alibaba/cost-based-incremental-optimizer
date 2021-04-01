package org.apache.calcite.util;

import java.util.*;


public class IdentityLinkedHashMap<K, V> implements Map<K, V>  {

    Map<KeyIdentity, V> elements = new LinkedHashMap<KeyIdentity, V>();

    @Override
    public int size() {
        return elements.size();
    }

    @Override
    public boolean isEmpty() {
        return elements.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return elements.containsKey(new KeyIdentity(key));
    }

    @Override
    public boolean containsValue(Object value) {
        return elements.containsValue(value);
    }

    @Override
    public V get(Object key) {
        return elements.get(new KeyIdentity(key));
    }

    @Override
    public V put(K key, V value) {
        return elements.put(new KeyIdentity(key), value);
    }

    @Override
    public V remove(Object key) {
        return elements.remove(new KeyIdentity(key));
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        for (Entry<? extends K, ? extends V> entry : m.entrySet()) {
            elements.put(new KeyIdentity(entry.getKey()), entry.getValue());
        }
    }

    @Override
    public void clear() {
        elements.clear();
    }

    @Override
    public Set<K> keySet() {
        return new IdentityLinkedKeySet();
    }

    @Override
    public Collection<V> values() {
        return elements.values();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return new IdentityLinkedEntrySet();
    }

    static class KeyIdentity {
        Object k;

        KeyIdentity(Object k) {
            this.k = k;
        }

        @Override
        public boolean equals(Object o) {
            return o.getClass() == KeyIdentity.class && ((KeyIdentity) o).k == k;

        }

        @Override
        public int hashCode() {
            return System.identityHashCode(k);
        }
    }

    class IdentityLinkedKeySet extends AbstractSet<K> {

        @Override
        public Iterator<K> iterator() {
            return new IdentityLinkedKeySetIterator(elements.keySet().iterator());
        }

        @Override
        public boolean remove(Object o) {
            return elements.remove(new KeyIdentity(o)) != null;
        }

        @Override
        public int size() {
            return IdentityLinkedHashMap.this.size();
        }

        @Override
        public boolean contains(Object o) {
            return o instanceof KeyIdentity && containsKey(((KeyIdentity) o).k);
        }
    }

    private class IdentityLinkedKeySetIterator implements Iterator<K> {
        Iterator<KeyIdentity> iterator;

        public IdentityLinkedKeySetIterator(Iterator<KeyIdentity> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public K next() {
            return (K) iterator.next().k;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private class IdentityLinkedEntrySet extends AbstractSet<Entry<K, V>> {
        @Override
        public Iterator<Entry<K, V>> iterator() {
            return new IdentityLinkedEntrySetIterator(elements.entrySet().iterator());
        }

        @Override
        public int size() {
            return IdentityLinkedHashMap.this.size();
        }

        @Override
        public boolean contains(Object o) {
            return o instanceof Entry && ((Entry) o).getKey() instanceof KeyIdentity
                    && containsKey(((KeyIdentity) ((Entry) o).getKey()).k);
        }
    }

    private class IdentityLinkedEntrySetIterator implements Iterator<Entry<K, V>> {
        Iterator<Entry<KeyIdentity, V>> entries;

        public IdentityLinkedEntrySetIterator(Iterator<Entry<KeyIdentity, V>> entries) {
            this.entries = entries;
        }

        @Override
        public boolean hasNext() {
            return entries.hasNext();
        }

        @Override
        public Entry<K, V> next() {
            Entry<KeyIdentity, V> next = entries.next();
            return new IdentityLinkedMapEntry<K, V>((K)next.getKey().k, next.getValue());
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private class IdentityLinkedMapEntry<K, V> implements Entry<K, V> {
            K k;
            V value;

            public IdentityLinkedMapEntry(K k, V value) {
                this.k = k;
                this.value = value;
            }

            @Override
            public K getKey() {
                return k;
            }

            @Override
            public V getValue() {
                return value;
            }

            @Override
            public V setValue(V value) {
                V old = this.value;
                this.value = value;
                return old;
            }
        }
    }
}
