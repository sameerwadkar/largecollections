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
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;

import utils.SerializationUtils;
import utils.DBUtils;

import com.google.common.base.Throwables;

/**
 * CacheMap is a true implementation of Map. Unlike FastHashMap which operates about 30% faster on put() and remove() CacheMap returns the
 * correct value for size() function. OffHeapMap provides a heuristic value for size which is compensated by its faster performance.
 */
public class CacheMap<K, V>  implements Map<K,V>,IDb, Serializable,Closeable{
    public  static final long serialVersionUID = 2l;
    private final static Random rnd = new Random();
    

    protected String folder = Constants.DEFAULT_FOLDER;
    protected int cacheSize = Constants.DEFAULT_CACHE_SIZE;
    protected String name = null;    
    private int size=0;    
    protected transient DB db;
    protected transient Options options;
    protected transient File dbFile;
    

    protected transient SerializationUtils<K,V> utils = new SerializationUtils<K,V>();
    
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
        return db.get(utils.serializeKey((K)key)) != null;
    }

    public boolean containsValue(Object value) {
        // Not implemented as another seperate DB would be needed to store this information
        // Perhaps next version
        throw new UnsupportedOperationException();

    }

    public V get(Object key) {
        // TODO Auto-generated method stub
        if (key == null) {
            return null;
        }
        byte[] vbytes = db.get(utils.serializeKey((K)key));
        if(vbytes==null){
            return null;
        }
        else{
            return  utils.deserializeValue(vbytes);    
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
            db.put(utils.serializeKey(key), utils.serializeValue(value));
            size++;
            
        }
        else{
            db.put(utils.serializeKey(key), utils.serializeValue(value));
        }
        return value;
    }

    public V remove(Object key) {
        V v = this.get(key);
        if(v!=null){
            db.delete(utils.serializeKey((K)key));
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
                batch.put((utils.serializeKey(e.getKey())),
                        utils.serializeValue(e.getValue()));
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
        Map m = DBUtils.createDB(this.folder, this.name, this.cacheSize);
        this.db = (DB)m.get(Constants.DB_KEY);
        this.options = (Options)m.get(Constants.DB_OPTIONS_KEY);
        this.dbFile = (File) m.get(Constants.DB_FILE_KEY);
        this.utils = new SerializationUtils<K,V>();
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
        return new MapEntrySet<K,V>(this);
    }
    
    
    public void close() throws IOException {
        try{
            this.db.close();
            factory.destroy(this.dbFile, this.options);
        }
        catch(Exception ex){
            throw Throwables.propagate(ex);
        }
    }
    
    public DB getDB() {
        return this.db;
    }
}
