package org.largecollections;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.iq80.leveldb.DB;

public  class MapSet<K> implements Set<K> {
    private Map<K,?> map = null;
    private DB db = null;
    protected MapSet(Map<K,?> map) {
        this.map = map;
        this.db = ((IMap)this.map).getDB();
    }

    public int size() {
        return map.size();
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public boolean contains(Object o) {
        return map.containsKey(o);
    }

    public Iterator<K> iterator() {
        return new KeyIterator<K>(this.db);
    }

    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException();
    }

    public boolean add(K e) {
        throw new UnsupportedOperationException();
    }

    public boolean remove(Object o) {
        this.map.remove(o);
        return true;
    }

  

    public void clear() {
        this.map.clear();
    }

    public boolean containsAll(Collection<?> c) {
        return false;
    }

    public boolean addAll(Collection<? extends K> c) {
        return false;
    }

    public boolean retainAll(Collection<?> c) {
        return false;
    }

    public boolean removeAll(Collection<?> c) {
        return false;
    }

}
