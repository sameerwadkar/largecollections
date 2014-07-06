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
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;

import utils.DBUtils;
import utils.SerializationUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

/**
 * CacheMap is a true implementation of Map. Unlike FastHashMap which operates
 * about 30% faster on put() and remove() CacheMap returns the correct value for
 * size() function. OffHeapMap provides a heuristic value for size which is
 * compensated by its faster performance.
 */
public class CacheMap<K, V> implements Map<K, V>, IDb, Serializable, Closeable {
    public static final long serialVersionUID = 2l;
    private final static Random rnd = new Random();

    protected String folder = Constants.DEFAULT_FOLDER;
    protected int cacheSize = Constants.DEFAULT_CACHE_SIZE;
    protected String name = null;
    private int size = 0;
    protected transient DB db;
    protected transient Options options;
    protected transient File dbFile;

    private SerializationUtils<K, V> serdeUtils = new SerializationUtils<K, V>();
    private int bloomFilterSize = 10000000;
    private BloomFilter<byte[]> bloomFilter = BloomFilter.create(
            Funnels.byteArrayFunnel(), this.bloomFilterSize);

    public CacheMap(String folder, String name, int cacheSize,
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

    public CacheMap(String folder, String name, int cacheSize) {
        this(folder, name, cacheSize, null);
    }

    public CacheMap(String folder, String name) {
        this(folder, name, Constants.DEFAULT_CACHE_SIZE, null);
    }

    public CacheMap(String folder) {
        this(folder, "TMP" + rnd.nextInt(1000000),
                Constants.DEFAULT_CACHE_SIZE, null);
    }

    public CacheMap() {
        this(Constants.DEFAULT_FOLDER, "TMP" + rnd.nextInt(1000000),
                Constants.DEFAULT_CACHE_SIZE, null);
    }

    public void setBloomFilterSize(int bFilterSize) {
        Preconditions
                .checkState(this.size() == 0,
                        "Cannot reset bloom filter size when the map has non-zero size");
        Preconditions.checkArgument(bFilterSize <= 0,
                "Bloom Filter must have a non-zero estimated size");
        this.bloomFilterSize = bFilterSize;
        this.bloomFilter = BloomFilter.create(Funnels.byteArrayFunnel(),
                this.bloomFilterSize);
    }

    public boolean containsKey(Object key) {
        byte[] keyBytes = this.serdeUtils.serializeKey((K) key);
        byte[] valBytes = null;
        if (bloomFilter.mightContain(keyBytes)) {
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
        byte[] keyArr = serdeUtils.serializeKey((K) key);
        if (bloomFilter.mightContain(keyArr)) {
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
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public V put(K key, V value) {
        if (!this.containsKey(key)) {
            byte[] fullKeyArr = serdeUtils.serializeKey(key);
            bloomFilter.put(fullKeyArr);
            db.put(fullKeyArr, serdeUtils.serializeValue(value));
            size++;
        } else {
            db.put(serdeUtils.serializeKey(key),
                   serdeUtils.serializeValue(value));
        }
        return value;
    }

    public V remove(Object key) {
        V v = this.get(key);
        if (v != null) {
            db.delete(serdeUtils.serializeKey((K) key));
            size--;

        }
        return v;
    }

    public void putAll(Map<? extends K, ? extends V> m) {
        try {
            WriteBatch batch = db.createWriteBatch();
            int counter = 0;
            for (Map.Entry<? extends K, ? extends V> e : m.entrySet()) {
                byte[] keyArr = serdeUtils.serializeKey(e.getKey());
                V v = this.get(e.getKey());
                if (this.size > 0) {
                    v = this.get(e.getKey());
                }
                if (v == null) {  
                    bloomFilter.put(keyArr);
                    this.size++;
                }
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
        stream.writeInt(this.size);
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
        this.size = in.readInt();
        this.serdeUtils = (SerializationUtils<K, V>) in.readObject();
        this.bloomFilterSize = in.readInt();
        this.bloomFilter = (BloomFilter<byte[]>) in.readObject();
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
            this.size--;
        }
        bloomFilter= BloomFilter.create(Funnels.byteArrayFunnel(), bloomFilterSize);
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
