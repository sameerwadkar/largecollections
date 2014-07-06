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

import static org.fusesource.leveldbjni.JniDBFactory.bytes;
import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.Range;
import org.iq80.leveldb.WriteBatch;

import utils.DBUtils;
import utils.SerializationUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

/**
 * FastHashMap is an implementation of java.util.Map. FastHashMap provides a heuristic value for size which is compensated by its 
 * faster performance. If you want a true implementation of Map which returns the most accurate value for size() use CacheMap or 
 * LargeCacheMap
 * 
 * It differs from CacheMap in that it provides a lsize() function which is longer version of size(). In this respect it deviates from a
 * typical Map implementation. In return it allows LargeCacheMap to have size higher than Integer.MAX_VALUE
 * 
 */
public class FastCacheMap<K, V> implements Map<K, V>, IDb, Serializable, Closeable {
    public static final long serialVersionUID = 2l;
    private final static Random rnd = new Random();

    protected String folder = Constants.DEFAULT_FOLDER;
    protected int cacheSize = Constants.DEFAULT_CACHE_SIZE;
    protected String name = null;
    protected transient DB db;
    protected transient Options options;
    protected transient File dbFile;

    private SerializationUtils<K, V> serdeUtils = new SerializationUtils<K, V>();
    private int bloomFilterSize = 10000000;
    protected  transient Funnel<K> myFunnel = null;
    private BloomFilter<K> bloomFilter = null;
    
    private void initializeBloomFilter(){
        this.myFunnel = new Funnel<K>() {
            public void funnel(K obj, PrimitiveSink into) {
                into.putInt(Math.abs(obj.hashCode()));
                  
              }
            };  
        this.bloomFilter = BloomFilter.create(myFunnel, this.bloomFilterSize);
    }

    public FastCacheMap(String folder, String name, int cacheSize) {
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
            this.initializeBloomFilter();

        } catch (Exception ex) {
            Throwables.propagate(ex);
        }

    }


    public FastCacheMap(String folder, String name) {
        this(folder, name, Constants.DEFAULT_CACHE_SIZE);
    }

    public FastCacheMap(String folder) {
        this(folder, "TMP" + rnd.nextInt(1000000),
                Constants.DEFAULT_CACHE_SIZE);
    }

    public FastCacheMap() {
        this(Constants.DEFAULT_FOLDER, "TMP" + rnd.nextInt(1000000),
                Constants.DEFAULT_CACHE_SIZE);
    }

    public void setBloomFilterSize(int bFilterSize) {
        Preconditions
                .checkState(this.size() == 0,
                        "Cannot reset bloom filter size when the map has non-zero size");
        Preconditions.checkArgument(bFilterSize <= 0,
                "Bloom Filter must have a non-zero estimated size");
        this.bloomFilterSize = bFilterSize;
        this.initializeBloomFilter();

    }

    public boolean containsKey(Object key) {
        
        byte[] valBytes = null;
        if (bloomFilter.mightContain((K)key)) {
            byte[] keyBytes = this.serdeUtils.serializeKey((K) key);
            valBytes = db.get(keyBytes);
        }
        return valBytes != null;
    }

    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();

    }

    public V get(Object key) {
        if (key == null) {
            throw new RuntimeException("Nulls are not allowed as key");
        } 
        
        if (bloomFilter.mightContain((K) key)) {
            byte[] keyArr = serdeUtils.serializeKey((K) key);
            byte[] vbytes = db.get(keyArr);
            if (vbytes == null) {
                return null;
            } else {
                return serdeUtils.deserializeValue(vbytes);
            }
        } else {
            return null;
        }

    }

    public int size() {
        // Not reliable. Only an approximation
        Range r = new Range(bytes(Character.toString(Character.MIN_VALUE)),
                bytes(Character.toString(Character.MAX_VALUE)));
        long[] l = this.db.getApproximateSizes(r);
        if (l != null && l.length > 0) {
            return (int) l[0];
        }
        return 0;
    }
    public boolean isEmpty() {
        return !this.keySet().iterator().hasNext();
    }

    public V put(K key, V value) {
        db.put(serdeUtils.serializeKey(key),
                serdeUtils.serializeValue(value));
        bloomFilter.put(key);
        return value;
    }

    public V remove(Object key) {
        db.delete(serdeUtils.serializeKey((K) key));
        return null;//Breaks the interface a little but fast
    }

    public void putAll(Map<? extends K, ? extends V> m) {
        try {
            WriteBatch batch = db.createWriteBatch();
            int counter = 0;
            for (Map.Entry<? extends K, ? extends V> e : m.entrySet()) {
                byte[] keyArr = serdeUtils.serializeKey(e.getKey());

                bloomFilter.put(e.getKey());
                batch.put(keyArr,
                        serdeUtils.serializeValue(e.getValue()));
                counter++;
                if (counter % 1000 == 0) {
                    db.write(batch);
                    batch.close();
                    batch = db.createWriteBatch();
                }
            }
        } catch (Exception ex) {
            Throwables.propagate(ex);
        }

    }

    private void writeObject(java.io.ObjectOutputStream stream)
            throws IOException {
        stream.writeObject(this.folder);
        stream.writeObject(this.name);
        stream.writeInt(this.cacheSize);
        stream.writeObject(this.serdeUtils);
        stream.writeInt(this.bloomFilterSize);
        stream.writeObject(this.bloomFilter);
        this.db.close();

    }

    private void readObject(java.io.ObjectInputStream in) throws IOException,
            ClassNotFoundException {
        this.folder = (String) in.readObject();
        this.name = (String) in.readObject();
        this.cacheSize = in.readInt();
        this.serdeUtils = (SerializationUtils<K, V>) in.readObject();
        this.bloomFilterSize = in.readInt();
        this.bloomFilter = (BloomFilter<K>) in.readObject();
        Map m = DBUtils.createDB(this.folder, this.name, this.cacheSize);
        this.db = (DB) m.get(Constants.DB_KEY);
        this.options = (Options) m.get(Constants.DB_OPTIONS_KEY);
        this.dbFile = (File) m.get(Constants.DB_FILE_KEY);
    }

    public void clear() {
        DBIterator iter = this.db.iterator();
        iter.seekToFirst();
        while (iter.hasNext()) {
            Entry<byte[], byte[]> e = iter.next();
            this.db.delete(e.getKey());
        }
        this.initializeBloomFilter();
    }

    public Set<K> keySet() {
        return new MapKeySet<K>(this);
    }

    public Collection<V> values() {
        return new MapCollection<V>(this);
    }

    public Set<java.util.Map.Entry<K, V>> entrySet() {
        return new MapEntrySet<K, V>(this);
    }

    public void close() throws IOException {
        try {
            this.initializeBloomFilter();
            this.db.close();
            factory.destroy(this.dbFile, this.options);
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }

    public DB getDB() {
        return this.db;
    }
}
