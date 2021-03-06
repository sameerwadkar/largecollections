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
import utils.KeyUtils;
import utils.SerializationUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.google.common.hash.PrimitiveSink;

public class MapFactory<K, V> implements Serializable, Closeable {
    public static final long serialVersionUID = 6l;
    private final static Random rnd = new Random();

    protected String folder = Constants.DEFAULT_FOLDER;
    protected int cacheSize = Constants.DEFAULT_CACHE_SIZE;
    protected String name = null;
    private int bloomFilterSize=10000000;
    protected transient DB db;
    protected transient Options options;
    protected transient File dbFile;

    private Map<String, Map<K, V>> myMaps = new HashMap<String, Map<K, V>>();
    protected  transient Funnel<K> myFunnel = null;
    private BloomFilter<K> bloomFilter = null;
    public MapFactory() {
        this(Constants.DEFAULT_FOLDER, "TMP" + rnd.nextInt(1000000),
                Constants.DEFAULT_CACHE_SIZE);

    }

    public MapFactory(String folderName, String name) {
        this(folderName, name, Constants.DEFAULT_CACHE_SIZE);
    }

    private void initializeBloomFilter(){
        this.myFunnel = new Funnel<K>() {
            public void funnel(K obj, PrimitiveSink into) {
                into.putInt(Math.abs(obj.hashCode()));
                  
              }
            };  
        this.bloomFilter = BloomFilter.create(myFunnel, this.bloomFilterSize);
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
            this.db = (DB) m.get(Constants.DB_KEY);
            this.options = (Options) m.get(Constants.DB_OPTIONS_KEY);
            this.dbFile = (File) m.get(Constants.DB_FILE_KEY);
            this.initializeBloomFilter();
            
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }

    public void setBloomFilterSize(int bFilterSize) {
        Preconditions.checkState(this.myMaps.size()==0,"Cannot reset bloom filter after Factory has generated maps");
        Preconditions.checkArgument(bFilterSize<=0, "Bloom Filter must have a non-zero estimated size");
        this.bloomFilterSize=bFilterSize;
        this.initializeBloomFilter();
    }

    public Map<K, V> getMap(String cacheName) {
        if (myMaps.get(cacheName) == null) {
            Map<K, V> m = new InnerMap<K, V>(cacheName, db,this.bloomFilter);
            myMaps.put(cacheName, m);
        }
        return myMaps.get(cacheName);
    }

    public void close() {
        try {
            this.myMaps.clear();
            this.initializeBloomFilter();
            this.db.close();
            factory.destroy(this.dbFile, this.options);

        } catch (Exception ex) {
            Throwables.propagate(ex);
        }
    }
    
    public void optimize() {
        try {
            this.initializeBloomFilter();
            for(Entry<String,Map<K,V>> entry:this.myMaps.entrySet()){
                InnerMap m = (InnerMap) entry.getValue();
                Set<K> s = m.keySet();
                for(K key:s){
                    this.bloomFilter.put(key);
                }
            }
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }
    

    private void writeObject(java.io.ObjectOutputStream stream)
            throws IOException {
        stream.writeObject(this.folder);
        stream.writeObject(this.name);
        stream.writeInt(this.cacheSize);        
        stream.writeInt(this.bloomFilterSize);
        stream.writeObject(this.bloomFilter);
        stream.writeObject(this.myMaps);
        this.db.close();
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException,
            ClassNotFoundException {
        this.folder = (String) in.readObject();
        this.name = (String) in.readObject();
        this.cacheSize = in.readInt();
        
        this.bloomFilterSize = in.readInt();
        this.bloomFilter = (BloomFilter<K>) in.readObject();
        this.myMaps = (Map<String, Map<K, V>>) in.readObject();
        Map m = DBUtils.createDB(this.folder, this.name, this.cacheSize);
        this.db = (DB) m.get(Constants.DB_KEY);
        this.options = (Options) m.get(Constants.DB_OPTIONS_KEY);
        this.dbFile = (File) m.get(Constants.DB_FILE_KEY);
        for (Map.Entry<String, Map<K, V>> e : this.myMaps.entrySet()) {
            ((InnerMap) e.getValue()).setDb(this.db);
            ((InnerMap) e.getValue()).setBloomFilter(this.bloomFilter);
        }
    }

    private final class InnerMap<K, V> implements Map<K, V>, IDb, Serializable {
        public static final long serialVersionUID = 5l;
        protected String cacheName = null;
        private transient DB db;
        protected int size = 0;
        private SerializationUtils<K, V> serdeUtils = new SerializationUtils<K, V>();
        private BloomFilter<K> bFilter = null;
        public InnerMap(String cacheName, DB db, BloomFilter<K> bFilter) {
            this.cacheName = cacheName;
            this.db = db;
            this.bFilter = bFilter;
        }

        protected void setBloomFilter(BloomFilter<K> bFilter){
            this.bFilter = bFilter;
        }
        public SerializationUtils<K, V> getSerDeUtils() {
            return this.serdeUtils;
        }

        public String getCacheName() {
            return this.cacheName;
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
            if (key == null) {
                throw new RuntimeException("Nulls are not allowed as key");
            }
            byte[] valBytes = null;      
           
            if(this.bFilter.mightContain((K)key)){
                byte[] keyArr = serdeUtils.serializeKey((K)key);
                byte[] keyBytes = KeyUtils.getPrefixedKey(this.cacheName, keyArr);
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

            if(this.bFilter.mightContain((K)key)){
                byte[] keyArr = serdeUtils.serializeKey((K)key);
                byte[] keyBytes = KeyUtils.getPrefixedKey(this.cacheName, keyArr);

                byte[] vbytes = db.get(keyBytes);

                if (vbytes == null) {
                    return null;
                } else {
                    return (V) serdeUtils.deserializeValue(vbytes);
                }
            }
            else{
                return null;
            }
            //byte[] keyBytes = serdeUtils.serializeKey(this.cacheName, (K) key);



        }

        public int size() {
            return size;
        }

        public boolean isEmpty() {
            return (size == 0);
        }

        public V put(K key, V value) {
            if(key==null || value==null){
                throw new RuntimeException("Nulls are not allowed as key or value");
            }
            byte[] keyArr = serdeUtils.serializeKey(key);
            byte[] fullKeyArr = KeyUtils.getPrefixedKey(this.cacheName, keyArr);
            byte[] valArr = serdeUtils.serializeValue(value);
            if (!this.containsKey(key)) {                
                this.bFilter.put(key);
                size++;
            }
            db.put(fullKeyArr, valArr);
            return value;
        }

        public V remove(Object key) {
            
            V v = null;
            if (this.bFilter.mightContain((K)key)) {
                v = this.get(key);
                byte[] keyArr = serdeUtils.serializeKey((K)key);
                byte[] fullKeyArr = KeyUtils.getPrefixedKey(this.cacheName, keyArr);
                db.delete(fullKeyArr);
                size--;
            }
            return v;
        }

        public void putAll(Map<? extends K, ? extends V> m) {
            try {
                WriteBatch batch = db.createWriteBatch();
                int counter = 0;
                for (Map.Entry<? extends K, ? extends V> e : m.entrySet()) {
                    V v = null;
                    byte[] keyArr = serdeUtils.serializeKey(e.getKey());
                    byte[] keyBytes = KeyUtils.getPrefixedKey(this.cacheName, keyArr);
                    if (this.size > 0 && this.bFilter.mightContain(e.getKey())) {
                        v = this.get(e.getKey());
                    }
                    if (v == null) {  
                        this.bFilter.put(e.getKey());
                        this.size++;
                    }                    
                    batch.put(keyBytes,
                            serdeUtils.serializeValue(e.getValue()));
                    counter++;
                    if (counter % 1000 == 0) {// Write every 1000 batches.
                        db.write(batch);
                        batch.close();
                        batch = db.createWriteBatch();
                    }
                }
                db.write(batch);
                batch.close();
            } catch (Exception ex) {
                Throwables.propagate(ex);
            }

        }

        private void writeObject(java.io.ObjectOutputStream stream)
                throws IOException {

            stream.writeObject(this.cacheName);
            stream.writeInt(this.size);
            stream.writeObject(this.serdeUtils);

        }

        private void readObject(java.io.ObjectInputStream in)
                throws IOException, ClassNotFoundException {
            this.cacheName = (String) in.readObject();
            this.size = in.readInt();
            this.serdeUtils = (SerializationUtils<K, V>) in.readObject();
        }

        public void clear() {
            
            DBIterator iter = this.db.iterator();
            iter.seekToFirst();
            
            while (iter.hasNext()) {
                Entry<byte[], byte[]> e = iter.next();
                String prefix = new String(KeyUtils.getPrefixAndKey(e.getKey())[0]);
                if(prefix.equals(this.cacheName)){
                    this.db.delete(e.getKey());
                    this.size--;
                }
            }
            
            //Bloom Filter
            //this.= BloomFilter.create(Funnels.byteArrayFunnel(), bloomFilterSize);
            
        }

        public Set<K> keySet() {
            return new InnerMapKeySet<K>(this);
        }

        public Collection<V> values() {
            return new InnerMapCollection<V>(this);
        }

        public Set<java.util.Map.Entry<K, V>> entrySet() {
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

                throw new UnsupportedOperationException();
            }

            public Iterator<V> iterator() {
                return new MyValueIterator<V>(this.map);
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
                throw new UnsupportedOperationException();
                
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
            private SerializationUtils serdeUtils = null;

            public InnerMapKeySet(InnerMap map) {
                this.map = map;
                this.serdeUtils = map.getSerDeUtils();
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
                return new MyKeyIterator<K>(this.map);
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
                this.iterator = new MyEntryIterator<K, V>(map);
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
        private final class MyKeyIterator<K> implements Iterator<K> {
            private MyEntryIterator<K,V> entryIterator = null;
            public MyKeyIterator(InnerMap<K,V> map){
                entryIterator = new MyEntryIterator<K,V>(map);
            }
            public boolean hasNext() {

                return entryIterator.hasNext;
            }

            public K next() {

                return entryIterator.next().getKey();
            }

            public void remove() {
                 entryIterator.remove();
                
            }
        }
        private final class MyValueIterator<V> implements Iterator<V> {
            private MyEntryIterator<K,V> entryIterator = null;
            public MyValueIterator(InnerMap<K,V> map){
                entryIterator = new MyEntryIterator<K,V>(map);
            }
            public boolean hasNext() {

                return entryIterator.hasNext;
            }

            public V next() {

                return entryIterator.next().getValue();
            }

            public void remove() {
                 entryIterator.remove();
                
            }
        }
        
        private final class MyEntryIterator<K, V> implements
                Iterator<java.util.Map.Entry<K, V>> {

            private DBIterator iter = null;
            private String name = null;
            private SerializationUtils<K,V> serdeUtils = null;
            private boolean hasNext = false;
            Entry<byte[], byte[]> entry = null;
            private boolean markIteratorState(boolean hasNext,Entry<byte[],byte[]> e){
                this.hasNext = hasNext;
                this.entry = e;
                return this.hasNext;
            }
            private boolean manageIteratorState(Entry<byte[],byte[]> e){  
                byte[][] out = KeyUtils.getPrefixAndKey(e.getKey());       
                String prefix = new String(out[0]);
                if(prefix.equals(this.name)){
                    return this.markIteratorState(true, e);
                }
                else{
                    return this.markIteratorState(false, null);
                }
         }            
            public MyEntryIterator(InnerMap<K,V> map) {
                this.iter = map.getDb().iterator();
                this.iter.seekToFirst();
                this.name = map.getCacheName();
                this.serdeUtils = map.getSerDeUtils();
                while (iter.hasNext()) {
                    Entry<byte[],byte[]>e = this.iter.next();
                    if(this.manageIteratorState(e)){
                        break;
                    }
                }
            }

            public boolean hasNext() {
                return hasNext;
            }

            public java.util.Map.Entry<K, V> next() {
                java.util.Map.Entry<K, V>  retVal = null;
                if (this.hasNext) {
                    K key =  (K) serdeUtils.deserializeKey(KeyUtils.getKey(entry.getKey()));
                    V value =  (V) serdeUtils.deserializeValue(entry.getValue());
                    retVal = new SimpleEntry(key,value);
                   
                    if(this.iter.hasNext()){
                        this.manageIteratorState(this.iter.next());
                    }
                    else{
                        this.markIteratorState(false,null);
                    }
                    
                } else {
                    throw new NoSuchElementException();
                }
                return retVal;
               
            }

            public void remove() {
                if(this.hasNext){
                    this.iter.remove();
                }
                if(this.iter.hasNext()){
                    this.manageIteratorState(this.iter.next());
                }
                else{
                    this.markIteratorState(false,null);
                }
            }

        }

    }

}
