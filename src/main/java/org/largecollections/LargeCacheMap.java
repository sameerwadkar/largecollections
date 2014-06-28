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

import utils.DBUtils;
import utils.SerializationUtils;

import com.google.common.base.Throwables;
/**
 * LargeCacheMap is a true implementation of Map. 
 * 
 * It differs from CacheMap in that it provides a lsize() function which is longer version of size(). In this respect it deviates from a
 * typical Map implementation. In return it allows LargeCacheMap to have size higher than Integer.MAX_VALUE
 * 
 */
public class LargeCacheMap<K, V>  implements Map<K,V>, Serializable, Closeable{
    public  static final long serialVersionUID = 2l;

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
    private int size=0;
    private long longSize=0;    
    protected transient SerializationUtils<K,V> sdUtils = new SerializationUtils<K,V>();

    protected  DB createDB(String folderName, String name, int cacheSize,
            String comparatorCls) {
        //System.setProperty("java.io.timedir", folderName);
        try {
            this.options = new Options();
            options.cacheSize(cacheSize * 1048576); // 100MB cache
            

            if (comparatorCls != null) {
                Class c = Class.forName(comparatorCls);
                options.comparator((DBComparator) c.newInstance());
            }
            ///this.dbFile = File.createTempFile(name, null);
            this.dbFile = new File(this.folder+File.separator+this.name);
            if(!this.dbFile.exists()){
                this.dbFile.mkdirs();
            }
            
            //new File(folderName + File.separator + name)
            db = factory.open(this.dbFile,options);
        } catch (Exception ex) {
            Throwables.propagate(ex);
        }
        return db;

    }

    
    public DB createDB() {
        return createDB(this.folder, this.name, this.cacheSize,
                this.dbComparatorCls);
    }

    
    public LargeCacheMap(String folder, String name, int cacheSize,
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
            this.db = this.createDB(this.folder, this.name, this.cacheSize,
                    this.dbComparatorCls);

        } catch (Exception ex) {
            Throwables.propagate(ex);
        }

    }

    public LargeCacheMap(String folder, String name, int cacheSize) {
        this(folder, name, cacheSize, null);
    }

    public LargeCacheMap(String folder, String name) {
        this(folder, name, LargeCacheMap.DEFAULT_CACHE_SIZE, null);
    }

    public LargeCacheMap(String folder) {
        this(folder, "TMP" + rnd.nextInt(1000000), LargeCacheMap.DEFAULT_CACHE_SIZE,
                null);
    }

    public LargeCacheMap() {
        this(LargeCacheMap.DEFAULT_FOLDER, "TMP" + rnd.nextInt(1000000),
                LargeCacheMap.DEFAULT_CACHE_SIZE, null);
    }


    public boolean containsKey(Object key) {
        // TODO Auto-generated method stub
        return db.get(sdUtils.serializeKey((K)key)) != null;
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
        byte[] vbytes = db.get(sdUtils.serializeKey((K)key));
        if(vbytes==null){
            return null;
        }
        else{
            return sdUtils.deserializeValue(vbytes);    
        }
        
    }

    public int size() {
        return size;
    }

    public long lsize(){
        return this.longSize;
    }
    
    public boolean isEmpty() {
        return longSize==0;
    }

    public V put(K key, V value) {
        V v = this.get(key);
        if(v==null){
            db.put(sdUtils.serializeKey(key), sdUtils.serializeValue(value));
            size++;
            longSize++;
        }
        else{
            db.put(sdUtils.serializeKey(key), sdUtils.serializeValue(value));
        }
        return value;
    }

    public V remove(Object key) {
        V v = this.get(key);
        if(v!=null){
            db.delete(sdUtils.serializeKey((K)key));
            size--;
            longSize--;
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
                    this.longSize++;
                }
                batch.put((sdUtils.serializeKey(e.getKey())),
                           sdUtils.serializeValue(e.getValue()));
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
        stream.writeObject(this.dbComparatorCls);
        stream.writeInt(this.size);
        stream.writeLong(this.longSize);
        this.db.close();
        
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException,
            ClassNotFoundException {
        this.folder = (String) in.readObject();
        this.name = (String) in.readObject();
        this.cacheSize = in.readInt();
        this.dbComparatorCls = (String) in.readObject();
        this.size = in.readInt();
        this.longSize = in.readLong();
        this.createDB();
        sdUtils = new SerializationUtils<K,V>();
        
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

    private DB getDb() {
        return db;
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
}
