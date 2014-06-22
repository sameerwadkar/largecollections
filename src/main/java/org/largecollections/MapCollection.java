package org.largecollections;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

public  class MapCollection<V> implements Collection<V> {
    private Map map = null;

    protected MapCollection(Map map) {
        this.map = map;
    }

    public int size() {
        // TODO Auto-generated method stub
        return this.map.size();
    }

    public boolean isEmpty() {
        // TODO Auto-generated method stub
        return this.map.isEmpty();
    }

    public boolean contains(Object o) {
        // TODO Auto-generated method stub
        return this.map.containsKey(o);
    }

    public Iterator<V> iterator() {
        throw new UnsupportedOperationException();
    }

    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException();
    }

    public boolean add(V e) {
        throw new UnsupportedOperationException();
    }

    public boolean remove(Object o) {
        // TODO Auto-generated method stub
        return (this.map.remove(o) != null);
    }

    public boolean containsAll(Collection<?> c) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    public boolean addAll(Collection<? extends V> c) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    public boolean removeAll(Collection<?> c) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    public boolean retainAll(Collection<?> c) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    public void clear() {
        // TODO Auto-generated method stub
        this.map.clear();
    }

}