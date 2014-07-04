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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;

import utils.DBUtils;
import utils.SerializationUtils;

import com.google.common.base.Throwables;

public class MapFactory<K, V> implements Serializable, Closeable {
    public  static final long serialVersionUID = 6l;
    private final static Random rnd = new Random();
    

    protected String folder = Constants.DEFAULT_FOLDER;
    protected int cacheSize = Constants.DEFAULT_CACHE_SIZE;
    protected String name = null;    
    protected transient DB db;
    protected transient Options options;
    protected transient File dbFile;
    
    private Map<String, Map<K, V>> myMaps = new HashMap<String, Map<K, V>>();
    
    
    public MapFactory() {
        this(Constants.DEFAULT_FOLDER, "TMP" + rnd.nextInt(1000000),
                Constants.DEFAULT_CACHE_SIZE);

    }
    
    public MapFactory(String folderName, String name) {
        this(folderName, name,
                Constants.DEFAULT_CACHE_SIZE);
    }
    
    public MapFactory(String folder, String name, int cacheSize) {
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
            throw Throwables.propagate(ex);
        }
    }

   
    public static void getInstance(String folder,String name,int cacheSize){
        
    }

    public Map<K, V> getMap(String cacheName) {
        if (myMaps.get(cacheName) == null) {
            Map<K, V> m = new InnerMap<K, V>(cacheName, db);
            myMaps.put(cacheName, m);
        }
        return myMaps.get(cacheName);
    }

    public void close() {
        try{
            this.db.close();
            factory.destroy(this.dbFile, this.options);
        }
        catch(Exception ex){
            throw Throwables.propagate(ex);
        }

    }

    private void writeObject(java.io.ObjectOutputStream stream)
            throws IOException {

        stream.writeObject(this.folder);
        stream.writeObject(this.name);
        stream.writeInt(this.cacheSize);
        stream.writeObject(this.myMaps);
        this.db.close();
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException,
            ClassNotFoundException {
        this.folder = (String) in.readObject();
        this.name = (String) in.readObject();
        this.cacheSize = in.readInt();
        this.myMaps = (Map<String, Map<K, V>>) in.readObject();
        Map m = DBUtils.createDB(this.folder, this.name, this.cacheSize);
        this.db = (DB)m.get(Constants.DB_KEY);
        this.options = (Options)m.get(Constants.DB_OPTIONS_KEY);
        this.dbFile = (File) m.get(Constants.DB_FILE_KEY);
        for (Map.Entry<String, Map<K, V>> e : this.myMaps.entrySet()) {
            ((InnerMap) e.getValue()).setDb(this.db);
        }
    }

    private final class InnerMap<K, V> implements Map<K, V>,IDb, Serializable{
        public static final long serialVersionUID = 5l;
        protected String cacheName = null;
        private transient DB db;
        protected int size = 0;
        protected transient SerializationUtils<K,V> serdeUtils = new SerializationUtils<K,V>();
        public InnerMap(String cacheName, DB db) {
            this.cacheName = cacheName;
            this.db = db;
        }

        public DB getDB() {
            return this.db;
        }

        public String getName() {
            return cacheName;
        }

        public void setName(String name) {
            this.cacheName = name;
        }

        public void setDb(DB db) {
            this.db = db;
        }

        public boolean containsKey(Object key) {
            byte[] keyBytes = serdeUtils.serializeKey(this.cacheName, (K)key);
            byte[] valBytes = db.get(keyBytes);
            return valBytes != null;
        }

        public boolean containsValue(Object value) {
            
            throw new UnsupportedOperationException();

        }

       
        public V get(Object key) {
            
            if (key == null) {
                return null;
            }
            byte[] keyBytes = serdeUtils.serializeKey(this.cacheName,(K) key);
            
            byte[] vbytes = db.get(keyBytes);
            
            if (vbytes == null) {
                return null;
            } else {
                return (V) serdeUtils.deserializeValue(vbytes);
            }

        }

        public int size() {
            return size;
        }


        public boolean isEmpty() {
            return (size == 0);
        }

        public V put(K key, V value) {
            byte[] keyArr = serdeUtils.serializeKey(this.cacheName, key);
            byte[] valArr = serdeUtils.serializeValue(value);
            if(!this.containsKey(key)){
                size++;
            }
            db.put(keyArr,valArr);
            return value;
        }

        public V remove(Object key) {
            V v = this.get(key);
            if (v != null) {
                db.delete(serdeUtils.serializeKey(this.cacheName,(K) key));
                size--;
            }
            return v;
        }

        public void putAll(Map<? extends K, ? extends V> m) {
            try {
                WriteBatch batch = db.createWriteBatch();
                int counter = 0;
                for (Map.Entry<? extends K, ? extends V> e : m.entrySet()) {
                    V v = this.get(e.getKey());
                    if (v == null) {
                        this.size++;
                    }
                    batch.put((serdeUtils.serializeKey(this.cacheName,(K)e.getKey())),
                            serdeUtils.serializeValue(e.getValue()));
                    counter++;
                    if (counter % 1000 == 0) {//Write every 1000 batches.
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

            stream.writeObject(this.cacheName);
            stream.writeInt(this.size);

        }

        private void readObject(java.io.ObjectInputStream in)
                throws IOException, ClassNotFoundException {
            this.cacheName = (String) in.readObject();
            this.size = in.readInt();
            this.serdeUtils = new SerializationUtils<K, V>();
        }

        public void clear() {
            Set<K> keys = this.keySet();
            for (K k : keys) {
                this.remove(k);
            }
        }

        public Set<K> keySet() {
            return new InnerMapKeySet<K>(this);
        }

        public Collection<V> values() {
            return new InnerMapCollection<V>(this);
        }

        public Set<java.util.Map.Entry<K, V>> entrySet() {
            // Return an Iterator backed by the DB
            return new InnerMapEntrySet<K, V>(this);
        }

        private DB getDb() {
            return db;
        }

        private final class InnerMapCollection<V> implements Collection<V> {
            private InnerMap<K, V> map = null;

            public InnerMapCollection(InnerMap<K, V> map) {
                this.map = map;
            }

            public int size() {
                
                return this.map.size();
            }

            public boolean isEmpty() {
                
                return this.map.isEmpty();
            }

            public boolean contains(Object o) {
                
                return this.map.containsKey(o);
            }

            public Iterator<V> iterator() {
                return new MapValueIterator<V>(this.map.getDb());
            }

            public Object[] toArray() {
                throw new UnsupportedOperationException();
            }

            public <T> T[] toArray(T[] a) {
                throw new UnsupportedOperationException();
            }

            public boolean add(V e) {
                throw new UnsupportedOperationException();
            }

            public boolean remove(Object o) {
                
                return (this.map.remove(o) != null);
            }

            public boolean containsAll(Collection<?> c) {
                
                throw new UnsupportedOperationException();
            }

            public boolean addAll(Collection<? extends V> c) {
                
                throw new UnsupportedOperationException();
            }

            public boolean removeAll(Collection<?> c) {
                
                this.map.clear();
                return true;
            }

            public boolean retainAll(Collection<?> c) {
                
                throw new UnsupportedOperationException();
            }

            public void clear() {
                
                this.map.clear();
            }

        }

        private final class InnerMapKeySet<K> implements Set<K> {
            private InnerMap map = null;

            public InnerMapKeySet(InnerMap map) {
                this.map = map;
            }

            public int size() {
                
                return map.size();
            }

            public boolean isEmpty() {
                
                return map.isEmpty();
            }

            public boolean contains(Object o) {

                return map.containsKey(o);
            }

            public Iterator<K> iterator() {
                
                return new MyKeyIterator<K>(this.map.getDb(),this.map.cacheName);
            }

            public Object[] toArray() {
                
                throw new UnsupportedOperationException();
            }

            public <T> T[] toArray(T[] a) {
                
                throw new UnsupportedOperationException();
            }

            public boolean add(K e) {
                
                throw new UnsupportedOperationException();
            }

            public boolean remove(Object o) {
                
                this.map.remove(o);
                return true;
            }

            public boolean containsAll(Collection<?> c) {
                
                if (c != null) {
                    for (Object o : c) {
                        if (this.map.get(o) == null) {
                            return false;
                        }
                    }
                }
                return true;
            }

            public boolean addAll(Collection<? extends K> c) {
                
                throw new UnsupportedOperationException();
            }

            public boolean retainAll(Collection<?> c) {
                
                throw new UnsupportedOperationException();
            }

            public boolean removeAll(Collection<?> c) {
                
                for (Object o : c) {
                    this.map.remove(o);
                }
                return true;
            }

            public void clear() {
                
                this.map.clear();
            }

        }

        private final class InnerMapEntrySet<K, V> extends
                AbstractSet<Map.Entry<K, V>> {
            private MyEntryIterator<K, V> iterator = null;
            private Map<K, V> map = null;

            public InnerMapEntrySet(InnerMap<K, V> map) {
                this.iterator = new MyEntryIterator<K, V>(map.getDb(),map.cacheName);
                this.map = map;
            }

            @Override
            public Iterator<java.util.Map.Entry<K, V>> iterator() {
                return this.iterator;
            }

            @Override
            public int size() {
                return this.map.size();
            }

        }

        private final class MyEntryIterator<K, V> implements
                Iterator<java.util.Map.Entry<K, V>> {

            private DBIterator iter = null;
            private String name = null;

            public MyEntryIterator(DB db,String name) {

                try {
                    this.iter = db.iterator();
                    this.name = name;
                    this.iter.seekToFirst();
                } catch (Exception ex) {
                    Throwables.propagate(ex);
                }
            }

            public boolean hasNext() {
                
                boolean hasNext = iter.hasNext();
                
                String k = "";
                Entry<byte[], byte[]> entry=null;
                while(iter.hasNext() && !k.startsWith(this.name+"\0")){
                    entry = this.iter.next();
                    k = new String(entry.getKey());
                }
                if(k.startsWith(this.name+"\0")){
                    this.iter.prev();
                }
                else{
                    hasNext=false;
                }
                
                return hasNext;
            }

            public java.util.Map.Entry<K, V> next() {
                
                if(this.hasNext()){
                    Entry<byte[], byte[]> entry = this.iter.next();
                    String k = new String(entry.getKey()).replaceAll(this.name+'\0', "");
                    return new SimpleEntry((K) serdeUtils.deserializeKey(k.getBytes()),
                            (V) serdeUtils.deserializeValue(entry.getValue()));
                }
                throw new NoSuchElementException();
                
            }

            public void remove() {
                this.iter.remove();
            }

        }

    }
    
    private final class MyKeyIterator<K> implements Iterator<K> {

        private DBIterator iter = null;
        private String name = null;
        protected transient SerializationUtils<K,V> serdeUtils = new SerializationUtils<K,V>();
        public MyKeyIterator(DB db,String name) {
            this.iter =db.iterator();
            this.iter.seekToFirst();
            this.name=name;
        }

        public boolean hasNext() {
            boolean hasNext = iter.hasNext();
            
            String k = "";
            Entry<byte[], byte[]> entry=null;
            while(iter.hasNext() && !k.startsWith(this.name+"\0")){
                entry = this.iter.next();
                k = new String(entry.getKey());
            }
            if(k.startsWith(this.name+"\0")){
                this.iter.prev();
            }
            else{
                hasNext=false;
            }
            
            return hasNext;        }

        public K next() {
            if(this.hasNext()){
                Entry<byte[], byte[]> entry = this.iter.next();
                String k = new String(entry.getKey()).replaceAll(this.name+'\0', "");
                return (K) serdeUtils.deserializeKey(k.getBytes());
            }
            throw new NoSuchElementException();
            
        }

        public void remove() {            
            this.iter.remove();
        }

    }
    
    
    public static void main(String[] args) throws Exception{
        MapFactory<String,String> factory = new MapFactory("c:/tmp/","test");
        Map<String,String> map1 = factory.getMap("test1");
        for(int i=0;i<5;i++){
            map1.put(Integer.toString(i), Integer.toString(i));
        }
        
        Map<String,String> map2 = factory.getMap("test2");
        for(int i=5;i<10;i++){
            map2.put(Integer.toString(i), Integer.toString(i));
        }
        
        map2.remove(Integer.toString(7));
        for(int i=0;i<5;i++){
            System.err.println("Map 1 Access: "+ map1.get(Integer.toString(i)));
            System.err.println("Map 2 Access: "+ map2.get(Integer.toString(i)));
        }
        
        
        for(int i=5;i<10;i++){
            System.err.println("Map 1 Access: "+ map1.get(Integer.toString(i)));
            System.err.println("Map 2 Access: "+ map2.get(Integer.toString(i)));
        }
        

        factory.close();
        
    }

}
