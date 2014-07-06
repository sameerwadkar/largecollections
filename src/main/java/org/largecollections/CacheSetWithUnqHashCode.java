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

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;

import utils.DBUtils;
import utils.SerializationUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

public class CacheSetWithUnqHashCode<K> implements Set<K>, Closeable, IDb {
    public static final long serialVersionUID = 10l;
    private final static Random rnd = new Random();

    protected String folder = Constants.DEFAULT_FOLDER;
    protected int cacheSize = Constants.DEFAULT_CACHE_SIZE;
    protected String name = null;
    private int size = 0;
    protected transient DB db;
    protected transient Options options;
    protected transient File dbFile;
    protected transient SerializationUtils<Integer, K> serdeUtils = new SerializationUtils<Integer, K>();
    private int bloomFilterSize = 10000000;
    protected  transient Funnel<Integer> myFunnel = null;
    private BloomFilter<Integer> bloomFilter = null;
    
    public void setBloomFilterSize(int bFilterSize) {
        Preconditions
                .checkState(this.size() == 0,
                        "Cannot reset bloom filter size when the map has non-zero size");
        Preconditions.checkArgument(bFilterSize <= 0,
                "Bloom Filter must have a non-zero estimated size");
        this.bloomFilterSize = bFilterSize;
        this.initializeBloomFilter();

    }

    private void initializeBloomFilter(){
        this.myFunnel = new Funnel<Integer>() {
            public void funnel(Integer obj, PrimitiveSink into) {
                into.putInt(obj);
                  
              }
            };  
        this.bloomFilter = BloomFilter.create(myFunnel, this.bloomFilterSize);
    }
    
    public CacheSetWithUnqHashCode(String folder, String name, int cacheSize,
            String comparatorCls) {
        try {
            if (!StringUtils.isEmpty(name)) {
                this.name = name;
            }

            if (!StringUtils.isEmpty(folder)) {
                this.folder = folder;
            }
            if (cacheSize > 0)
                this.cacheSize = cacheSize;
            Map m = DBUtils.createDB(this.folder, this.name, this.cacheSize);
            this.db = (DB) m.get(Constants.DB_KEY);
            this.options = (Options) m.get(Constants.DB_OPTIONS_KEY);
            this.dbFile = (File) m.get(Constants.DB_FILE_KEY);

        } catch (Exception ex) {
            Throwables.propagate(ex);
        }

    }

    public CacheSetWithUnqHashCode(String folder, String name, int cacheSize) {
        this(folder, name, cacheSize, null);
    }

    public CacheSetWithUnqHashCode(String folder, String name) {
        this(folder, name, Constants.DEFAULT_CACHE_SIZE, null);
    }

    public CacheSetWithUnqHashCode(String folder) {
        this(folder, "TMP" + rnd.nextInt(1000000),
                Constants.DEFAULT_CACHE_SIZE, null);
    }

    public CacheSetWithUnqHashCode() {
        this(Constants.DEFAULT_FOLDER, "TMP" + rnd.nextInt(1000000),
                Constants.DEFAULT_CACHE_SIZE, null);
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return (this.size == 0);
    }

    public boolean contains(Object key) {

        K v = serdeUtils.deserializeValue(db.get(Integer.toString(key.hashCode()).getBytes()));
        if (v != null) {
            return true;
        } else {
            return false;
        }

    }

    // @TODO - The Iterators need to reflect this change
    public Iterator<K> iterator() {
        return new MapValueIterator<K>(this.getDB());
    }

    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException();
    }

    public boolean add(K e) {
        boolean contains = this.contains(e);
        if (!contains) {
            size++;
        }
        db.put(Integer.toString(e.hashCode()).getBytes(), serdeUtils.serializeValue(e));
        return !contains;
    }

    public boolean remove(Object e) {
        boolean contains = this.contains(e);
        if (contains) {
            size--;
            db.delete(Integer.toString(e.hashCode()).getBytes());
        }
        return contains;
    }

    public void clear() {
        // Get all keys and delete all
        DBIterator iter = this.db.iterator();
        iter.seekToFirst();
        while (iter.hasNext()) {
            byte[] key = iter.next().getKey();
            this.db.delete(key);

        }
        this.size = 0;
    }

    public boolean containsAll(Collection<?> c) {
        boolean ret = true;
        Iterator<? extends K> i = (Iterator<? extends K>) c.iterator();
        while (i.hasNext()) {
            K v = i.next();
            if (!this.contains(v)) {
                ret = false;
                break;
            }
        }
        return ret;
    }

    public boolean addAll(Collection<? extends K> c) {
        Iterator<? extends K> i = c.iterator();
        int stSize = this.size;
        while (i.hasNext()) {
            K k = (K) i.next();
            this.add(k);
        }
        if (this.size > stSize) {
            return true;
        } else {
            return false;
        }

    }

    public boolean retainAll(Collection<?> c) {
        boolean changed = false;
        Iterator<? extends K> i = (Iterator<? extends K>) c.iterator();
        while (i.hasNext()) {
            K k = i.next();
            if (!this.contains(k)) {
                this.remove(k);
                changed = true;
            }
        }
        return changed;
    }

    public boolean removeAll(Collection<?> c) {
        boolean changed = false;
        Iterator<? extends K> i = (Iterator<? extends K>) c.iterator();
        while (i.hasNext()) {
            K k = i.next();
            if (this.contains(k)) {
                this.remove(k);
                changed = true;
            }
        }
        return changed;
    }

    public void close() throws IOException {
        try {
            this.db.close();
            factory.destroy(this.dbFile, this.options);
        } catch (Exception ex) {
            Throwables.propagate(ex);
        }

    }

    private void writeObject(java.io.ObjectOutputStream stream)
            throws IOException {
        stream.writeObject(this.folder);
        stream.writeObject(this.name);
        stream.writeInt(this.cacheSize);
        stream.writeInt(this.size);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException,
            ClassNotFoundException {
        this.folder = (String) in.readObject();
        this.name = (String) in.readObject();
        this.cacheSize = in.readInt();
        this.size = in.readInt();
        Map m = DBUtils.createDB(this.folder, this.name, this.cacheSize);
        this.db = (DB) m.get(Constants.DB_KEY);
        this.options = (Options) m.get(Constants.DB_OPTIONS_KEY);
        this.dbFile = (File) m.get(Constants.DB_FILE_KEY);
    }

    public DB getDB() {
        // TODO Auto-generated method stub
        return this.db;
    }

}
