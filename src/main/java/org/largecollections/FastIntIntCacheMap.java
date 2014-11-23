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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import com.google.common.primitives.Ints;

/**
 * CacheMap is a true implementation of Map. Unlike FastHashMap which operates
 * about 30% faster on put() and remove() CacheMap returns the correct value for
 * size() function. OffHeapMap provides a heuristic value for size which is
 * compensated by its faster performance.
 */
public class FastIntIntCacheMap implements IDb, Serializable, Closeable {
    public static final long serialVersionUID = 2l;
    private final static Random rnd = new Random();

    protected String folder = Constants.DEFAULT_FOLDER;
    protected int cacheSize = Constants.DEFAULT_CACHE_SIZE;
    protected String name = null;
    private int size = 0;
    protected transient DB db;
    protected transient Options options;
    protected transient File dbFile;

    //private SerializationUtils<K, V> serdeUtils = new SerializationUtils<K, V>();
    private int bloomFilterSize = 10000000;
    protected  transient Funnel<Integer> myFunnel = null;
    private BloomFilter<Integer> bloomFilter = null;
    
    private void initializeBloomFilter(){
        this.myFunnel = new Funnel<Integer>() {
            public void funnel(Integer obj, PrimitiveSink into) {
                into.putInt(Math.abs(obj.hashCode()));
                  
              }
            };  
        this.bloomFilter = BloomFilter.create(myFunnel, this.bloomFilterSize);
    }

    public FastIntIntCacheMap(String folder, String name, int cacheSize) {
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

    public FastIntIntCacheMap(String folder, String name) {
        this(folder, name, Constants.DEFAULT_CACHE_SIZE);
    }

    public FastIntIntCacheMap(String folder) {
        this(folder, "TMP" + rnd.nextInt(1000000));
    }

    public FastIntIntCacheMap() {
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

    /*
    public void optimize() {
        try {
            this.initializeBloomFilter();
            for(Entry<Integer,Integer> entry:this.entrySet()){
                this.bloomFilter.put(entry.getKey());
            }
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }
    */
    public boolean containsKey(int key) {
        byte[] valBytes = null;
        if (bloomFilter.mightContain(key)) {
            byte[] keyBytes = Ints.toByteArray(key);
            valBytes = db.get(keyBytes);
        }
        return valBytes != null;
    }

    public boolean containsValue(Object value) {
        //Will be very slow unless a seperate DB of values is maintained
        throw new UnsupportedOperationException();

    }

    public Integer get(int key) {
        
        if (bloomFilter.mightContain(key)) {
            byte[] keyArr = Ints.toByteArray(key);
            byte[] vbytes = db.get(keyArr);
            if (vbytes == null) {
                return null;
            } else {
                return Ints.fromByteArray(vbytes);
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

    public Integer put(int key, int value) {
        byte[] fullKeyArr = Ints.toByteArray(key);
        byte[] fullValArr = Ints.toByteArray(value);
        if (!this.containsKey(key)) {
            
            bloomFilter.put(key);
            db.put(fullKeyArr, Ints.toByteArray(value));
            size++;
        } else {
            db.put(fullKeyArr,fullValArr);
        }
        return value;
    }

    public Integer remove(int key) {

        Integer v = this.get(key);       
        if (v != null) {
            byte[] fullKeyArr = Ints.toByteArray((Integer)key);
            db.delete(fullKeyArr);
            size--;
        }
        return v;
    }

    public void putAll(Map<? extends Integer, ? extends Integer> m) {
        try {
            WriteBatch batch = db.createWriteBatch();
            int counter = 0;
            for (Map.Entry<? extends Integer, ? extends Integer> e : m.entrySet()) {
                byte[] keyArr = Ints.toByteArray(e.getKey());
                Integer v = this.get(e.getKey());
                if (this.size > 0  && this.bloomFilter.mightContain(e.getKey())) {
                    v = this.get(e.getKey());
                }
                if (v == null) {  
                    bloomFilter.put(e.getKey());
                    this.size++;
                }
                batch.put(keyArr,Ints.toByteArray(e.getValue()));
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
        this.bloomFilterSize = in.readInt();
        this.bloomFilter = (BloomFilter<Integer>) in.readObject();
        Map m = DBUtils.createDB(this.folder, this.name, this.cacheSize);
        this.db = (DB) m.get(Constants.DB_KEY);
        this.options = (Options) m.get(Constants.DB_OPTIONS_KEY);
        this.dbFile = (File) m.get(Constants.DB_FILE_KEY);
    }

    /*
    public void clear() {
        DBIterator iter = this.db.iterator();
        iter.seekToFirst();
        while (iter.hasNext()) {
            Entry<byte[], byte[]> e = iter.next();
            this.db.delete(e.getKey());
            this.size--;
        }
        this.initializeBloomFilter();
    }
    */
    /*
    public Set<Integer> keySet() {
        return new MapKeySet<Integer>(this);
    }

    public Collection<Integer> values() {
        return new MapCollection<Integer>(this);
    }

    public Set<java.util.Map.Entry<Integer, Integer>> entrySet() {
        return new MapEntrySet<Integer, Integer>(this);
    }
    */
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
