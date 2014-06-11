package org.largecollections;

import java.util.Iterator;
import java.util.Map.Entry;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;

import utils.Utils;

public final class ValueIterator<V> implements Iterator<V> {

    private DBIterator iter = null;

    public ValueIterator(DB db) {
        this.iter = db.iterator();
        this.iter.seekToFirst();
    }

    public boolean hasNext() {
        // TODO Auto-generated method stub
        return this.iter.hasNext();
    }

    public V next() {
        // TODO Auto-generated method stub
        Entry<byte[], byte[]> entry = this.iter.next();
        return (V) Utils.deserialize(entry.getValue());
    }

    public void remove() {
        // TODO Auto-generated method stub
        this.iter.remove();
    }

}