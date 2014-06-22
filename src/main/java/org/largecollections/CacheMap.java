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
import org.iq80.leveldb.WriteBatch;

import utils.Utils;

import com.google.common.base.Throwables;

/**
 * CacheMap is a true implementation of Map. Unlike FastHashMap which operates about 30% faster on put() and remove() CacheMap returns the
 * correct value for size() function. OffHeapMap provides a heuristic value for size which is compensated by its faster performance.
 */
public class CacheMap<K, V>  implements Map<K,V>,IMap, Serializable, Closeable{
    public  static final long serialVersionUID = 2l;
    private final static Random rnd = new Random();
    

    protected String folder = Constants.DEFAULT_FOLDER;
    protected int cacheSize = Constants.DEFAULT_CACHE_SIZE;
    protected String name = null;    
    private int size=0;    
    protected transient DB db;
    protected transient Options options;
    protected transient File dbFile;
    

    
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
            Map m = Utils.createDB(this.folder, this.name, this.cacheSize);
            this.db = (DB)m.get(Constants.DB_KEY);
            this.options = (Options)m.get(Constants.DB_OPTIONS_KEY);
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
        this(folder,"TMP" + rnd.nextInt(1000000), Constants.DEFAULT_CACHE_SIZE,
                null);
    }

    public CacheMap() {
        this(Constants.DEFAULT_FOLDER, "TMP" + rnd.nextInt(1000000),
                Constants.DEFAULT_CACHE_SIZE, null);
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

    public int size() {
        return size;
    }


    public boolean isEmpty() {
        return size==0;
    }

    public V put(K key, V value) {
        V v = this.get(key);
        if(v==null){
            db.put(Utils.serialize(key), Utils.serialize(value));
            size++;
            
        }
        else{
            db.put(Utils.serialize(key), Utils.serialize(value));
        }
        return value;
    }

    public V remove(Object key) {
        V v = this.get(key);
        if(v!=null){
            db.delete(Utils.serialize(key));
            size--;
            
        }
        return v;// Just a null to improve performance
    }

    public void putAll(Map<? extends K, ? extends V> m) {
        try {
            WriteBatch batch = db.createWriteBatch();
            int counter = 0;
            for (Map.Entry<? extends K, ? extends V> e : m.entrySet()) {
                V v = this.get(e.getKey());
                if(v==null){
                    this.size++;
                   
                }
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

    private void writeObject(java.io.ObjectOutputStream stream)
            throws IOException {
        stream.writeObject(this.folder);
        stream.writeObject(this.name);
        stream.writeInt(this.cacheSize);
        stream.writeInt(this.size);
      
        this.db.close();
        
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException,
            ClassNotFoundException {
        this.folder = (String) in.readObject();
        this.name = (String) in.readObject();
        this.cacheSize = in.readInt();
        this.size = in.readInt();
        Map m = Utils.createDB(this.folder, this.name, this.cacheSize);
        this.db = (DB)m.get(Constants.DB_KEY);
        this.options = (Options)m.get(Constants.DB_OPTIONS_KEY);
        this.dbFile = (File) m.get(Constants.DB_FILE_KEY);
    }

    public void clear() {
        Set<K> keys = this.keySet();
        for(K k:keys){
            this.remove(k);
        }
        

    }


    public Set<K> keySet() {
        return new MapSet<K>(this);
    }
    public Collection<V> values() {
        return new MapCollection<V>(this);
    }

    public Set<java.util.Map.Entry<K, V>> entrySet() {
        // Return an Iterator backed by the DB
        return new EntrySet<K,V>(this);
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


    public DB getDB() {
        // TODO Auto-generated method stub
        return this.db;
    }
}
