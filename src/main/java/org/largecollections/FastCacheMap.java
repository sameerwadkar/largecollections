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
import java.util.AbstractMap.SimpleEntry;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBComparator;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.Range;
import org.iq80.leveldb.WriteBatch;

import utils.Utils;

import com.google.common.base.Throwables;
/**
 * FastHashMap is an implementation of java.util.Map. FastHashMap provides a heuristic value for size which is compensated by its 
 * faster performance. If you want a true implementation of Map which returns the most accurate value for size() use CacheMap or 
 * LargeCacheMap
 * 
 * It differs from CacheMap in that it provides a lsize() function which is longer version of size(). In this respect it deviates from a
 * typical Map implementation. In return it allows LargeCacheMap to have size higher than Integer.MAX_VALUE
 * 
 */
public class FastCacheMap<K, V> implements Map<K, V>, Serializable,  Closeable,IMap{
    public  static final long serialVersionUID = 1l;
    private final static Random rnd = new Random();
    public static String DEFAULT_FOLDER = System.getProperty("java.io.tmpdir");
    //public static String DEFAULT_NAME = "TMP" + rnd.nextInt(1000000);
    public static int DEFAULT_CACHE_SIZE = 25;

    protected String folder = DEFAULT_FOLDER;
    protected String name = null;

    protected transient DB db;
    protected int cacheSize = DEFAULT_CACHE_SIZE;
    protected String dbComparatorCls = null;
    protected transient File dbFile = null;
    protected transient Options options = null;

    
    
    public FastCacheMap(String folder, String name, int cacheSize,
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
            this.dbComparatorCls = comparatorCls;
            Map m = Utils.createDB(this.folder, this.name, this.cacheSize);
            this.db = (DB)m.get(Constants.DB_KEY);
            this.options = (Options)m.get(Constants.DB_OPTIONS_KEY);
            this.dbFile = (File) m.get(Constants.DB_FILE_KEY);
        } catch (Exception ex) {
            Throwables.propagate(ex);
        }
    }

    public FastCacheMap(String folder, String name, int cacheSize) {
        this(folder, name, cacheSize, null);
    }

    public FastCacheMap(String folder, String name) {
        this(folder, name, FastCacheMap.DEFAULT_CACHE_SIZE, null);
    }

    public FastCacheMap(String folder) {
        this(folder, "TMP" + rnd.nextInt(1000000), FastCacheMap.DEFAULT_CACHE_SIZE,
                null);
    }

    public FastCacheMap() {
        this(FastCacheMap.DEFAULT_FOLDER, "TMP" + rnd.nextInt(1000000),
                FastCacheMap.DEFAULT_CACHE_SIZE, null);
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
        //return !this.db.iterator().hasNext();
    }

    public boolean containsKey(Object key) {
        // TODO Auto-generated method stub
        return db.get(Utils.serialize(key)) != null;
    }

    public boolean containsValue(Object value) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();

    }

    public V get(Object key) {
        // TODO Auto-generated method stub
        if (key == null) {
            return null;
        }
        byte[] vbytes = db.get(Utils.serialize(key));
        if(vbytes==null){
            return null;
        }
        else{
            return (V) Utils.deserialize(vbytes);    
        }
        
    }

    public V put(K key, V value) {
        // TODO Auto-generated method stub
        // First check is the entry exists
        db.put(Utils.serialize(key), Utils.serialize(value));
        return value;
    }

    public V remove(Object key) {
        // TODO Auto-generated method stub
        db.delete(Utils.serialize(key));
        return null;// Just a null to improve performance
    }

    public void putAll(Map<? extends K, ? extends V> m) {
        try {
            WriteBatch batch = db.createWriteBatch();
            int counter = 0;
            for (Map.Entry<? extends K, ? extends V> e : m.entrySet()) {
                batch.put((Utils.serialize(e.getKey())),
                        Utils.serialize(e.getValue()));
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

   
    public void clear() {
        Set<K> keys = this.keySet();
        for(K k:keys){
            this.remove(k);
        }
        

    }

   



    public Set<K> keySet() {
        return new MapKeySet<K>(this);
    }
    public Collection<V> values() {
        return new MapCollection<V>(this);
    }

    public Set<java.util.Map.Entry<K, V>> entrySet() {
        // Return an Iterator backed by the DB
        return new MapEntrySet<K,V>(this);
    }


    private void writeObject(java.io.ObjectOutputStream stream)
            throws IOException {
        stream.writeObject(this.folder);
        stream.writeObject(this.name);
        stream.writeInt(this.cacheSize);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException,
            ClassNotFoundException {
        this.folder = (String) in.readObject();
        this.name = (String) in.readObject();
        this.cacheSize = in.readInt();
        Map m = Utils.createDB(this.folder, this.name, this.cacheSize);
        this.db = (DB)m.get(Constants.DB_KEY);
        this.options = (Options)m.get(Constants.DB_OPTIONS_KEY);
        this.dbFile = (File) m.get(Constants.DB_FILE_KEY);
    }
    public void close() throws IOException {
        try{
            this.db.close();
            factory.destroy(this.dbFile, this.options);
        }
        catch(Exception ex){
            Throwables.propagate(ex);
        }
        
    }
    /**
     * @param args
     */

  
   
    public DB getDB() {
        return db;
    }







 

}
