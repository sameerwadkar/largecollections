/*
 * Copyright 2014 Sameer Wadkar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.largecollections;

import java.util.Iterator;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;

import utils.Utils;

import com.google.common.base.Throwables;

public final class MapEntryIterator<K, V> implements
        Iterator<java.util.Map.Entry<K, V>> {

    private DBIterator iter = null;

    protected MapEntryIterator(DB db) {

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
