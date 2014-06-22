package org.largecollections;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;

public class EntrySet<K, V> extends AbstractSet<Map.Entry<K, V>> {
    private EntryIterator<K, V> iterator = null;
    private Map<K, V> map = null;

    protected EntrySet(Map map) {
        this.iterator = new EntryIterator(((IMap)map).getDB());
        this.map = map;
    }

    @Override
    public Iterator<java.util.Map.Entry<K, V>> iterator() {
        // TODO Auto-generated method stub
        return this.iterator;
    }

    @Override
    public int size() {
        // TODO Auto-generated method stub
        return this.map.size();
    }

}