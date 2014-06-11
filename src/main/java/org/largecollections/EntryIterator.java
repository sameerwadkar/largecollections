package org.largecollections;

import java.util.Iterator;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;

import utils.Utils;

import com.google.common.base.Throwables;

public final class EntryIterator<K, V> implements
        Iterator<java.util.Map.Entry<K, V>> {

    private DBIterator iter = null;

    public EntryIterator(DB db) {

        try {
            this.iter = db.iterator();
            // this.iter.close();
            if (this.iter.hasPrev())
                this.iter.seekToLast();
            this.iter.seekToFirst();
        } catch (Exception ex) {
            Throwables.propagate(ex);
        }

    }

    public boolean hasNext() {
        // TODO Auto-generated method stub
        boolean hasNext = iter.hasNext();
        return hasNext;
    }

    public java.util.Map.Entry<K, V> next() {
        // TODO Auto-generated method stub
        Entry<byte[], byte[]> entry = this.iter.next();
        return new SimpleEntry((K) Utils.deserialize(entry.getKey()),
                (V) Utils.deserialize(entry.getValue()));
    }

    public void remove() {
        this.iter.remove();
    }

}
