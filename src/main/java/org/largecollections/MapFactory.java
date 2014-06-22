package org.largecollections;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBComparator;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;

import utils.Utils;

import com.google.common.base.Throwables;

public class MapFactory<K, V> implements Serializable, Closeable {
    public static final long serialVersionUID = 6l;
    private transient DB db = null;
    protected final static Random rnd = new Random();
    public static String DEFAULT_FOLDER = System.getProperty("java.io.tmpdir");
    // public static String DEFAULT_NAME = "TMP" + rnd.nextInt(1000000);
    public static int DEFAULT_CACHE_SIZE = 25;
    private Map<String, Map<K, V>> myMaps = new HashMap<String, Map<K, V>>();
    protected transient File dbFile = null;
    protected transient Options options = null;

    private String folder = DEFAULT_FOLDER;
    private String fName = null;
    private String comparatorCls = null;
    private int cacheSize = DEFAULT_CACHE_SIZE;

    protected DB createDB() {
        // System.setProperty("java.io.timedir", folderName);
        try {
            this.options = new Options();
            options.cacheSize(cacheSize * 1048576); // x1MB cache

            if (this.fName == null) {
                this.fName = "TMP" + rnd.nextInt(1000000);
            }
            this.dbFile = new File(this.folder, this.fName);

            // /this.dbFile = File.createTempFile(name, null);

            if (!this.dbFile.exists()) {
                this.dbFile.mkdirs();
            }

            // new File(folderName + File.separator + name)
            db = factory.open(this.dbFile, options);
        } catch (Exception ex) {
            Throwables.propagate(ex);
        }
        return db;

    }

    public MapFactory() {
        // Initialize DB
        this.createDB();

    }
    
    public MapFactory(String folderName, String name) {
        this.folder = folderName;
        this.fName = name;
        this.createDB();
    }
    
    public MapFactory(String folderName, String name, int cacheSize) {
        this.folder = folderName;
        this.fName = name;
        this.cacheSize = cacheSize;
        this.createDB();
    }

   

    public Map<K, V> getMap(String cacheName) {
        if (myMaps.get(cacheName) == null) {
            Map<K, V> m = new InnerMap<K, V>(cacheName, db);
            myMaps.put(cacheName, m);
        }
        return myMaps.get(cacheName);
    }

    public void close() throws IOException {
        this.db.close();

    }

    private void writeObject(java.io.ObjectOutputStream stream)
            throws IOException {

        stream.writeObject(this.folder);
        stream.writeObject(this.fName);
        stream.writeObject(this.comparatorCls);
        stream.writeInt(this.cacheSize);
        stream.writeObject(this.myMaps);
        this.db.close();
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException,
            ClassNotFoundException {
        this.folder = (String) in.readObject();
        this.fName = (String) in.readObject();
        this.comparatorCls = (String) in.readObject();
        this.cacheSize = in.readInt();
        this.myMaps = (Map<String, Map<K, V>>) in.readObject();
        this.createDB();
        for (Map.Entry<String, Map<K, V>> e : this.myMaps.entrySet()) {
            ((InnerMap) e.getValue()).setDb(this.db);
        }
    }

    private final class InnerMap<K, V> implements Map<K, V>, Serializable {
        public static final long serialVersionUID = 5l;
        protected String name = null;

        private transient DB db;
        protected int size = 0;
        protected long longSize = 0;

        public InnerMap(String cacheName, DB db) {
            this.name = cacheName;
            this.db = db;
        }

        private DB getDB() {
            return this.db;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setDb(DB db) {
            this.db = db;
        }

        public boolean containsKey(Object key) {
            // TODO Auto-generated method stub
            return db.get(Utils.serialize(this.name, key)) != null;
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
            byte[] keyBytes = Utils.serialize(this.name, key);
            
            byte[] vbytes = db.get(keyBytes);
            
            if (vbytes == null) {
                return null;
            } else {
                return (V) Utils.deserialize(vbytes);
            }

        }

        public int size() {
            return size;
        }

        public long lsize() {
            return this.longSize;
        }

        public boolean isEmpty() {
            return longSize == 0;
        }

        public V put(K key, V value) {
            byte[] keyArr = Utils.serialize(this.name, key);
            byte[] valArr = Utils.serialize(value);
            V v = this.get(key);
            if (v == null) {
                db.put(keyArr,valArr);
                size++;
                longSize++;
            } else {
                db.put(keyArr,valArr);
            }
            return value;
        }

        public V remove(Object key) {
            V v = this.get(key);
            if (v != null) {
                db.delete(Utils.serialize(this.name, key));
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
                    if (v == null) {
                        this.size++;
                        this.longSize++;
                    }
                    batch.put((Utils.serialize(this.name, e.getKey())),
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

            stream.writeObject(this.name);
            stream.writeInt(this.size);
            stream.writeLong(this.longSize);

        }

        private void readObject(java.io.ObjectInputStream in)
                throws IOException, ClassNotFoundException {
            this.name = (String) in.readObject();
            this.size = in.readInt();
            this.longSize = in.readLong();
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
                // TODO Auto-generated method stub
                return this.map.size();
            }

            public boolean isEmpty() {
                // TODO Auto-generated method stub
                return this.map.isEmpty();
            }

            public boolean contains(Object o) {
                // TODO Auto-generated method stub
                return this.map.containsKey(o);
            }

            public Iterator<V> iterator() {
                return new ValueIterator<V>(this.map.getDb());
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
                // TODO Auto-generated method stub
                return (this.map.remove(o) != null);
            }

            public boolean containsAll(Collection<?> c) {
                // TODO Auto-generated method stub
                throw new UnsupportedOperationException();
            }

            public boolean addAll(Collection<? extends V> c) {
                // TODO Auto-generated method stub
                throw new UnsupportedOperationException();
            }

            public boolean removeAll(Collection<?> c) {
                // TODO Auto-generated method stub
                this.map.clear();
                return true;
            }

            public boolean retainAll(Collection<?> c) {
                // TODO Auto-generated method stub
                throw new UnsupportedOperationException();
            }

            public void clear() {
                // TODO Auto-generated method stub
                this.map.clear();
            }

        }

        private final class InnerMapKeySet<K> implements Set<K> {
            private InnerMap map = null;

            public InnerMapKeySet(InnerMap map) {
                this.map = map;
            }

            public int size() {
                // TODO Auto-generated method stub
                return map.size();
            }

            public boolean isEmpty() {
                // TODO Auto-generated method stub
                return map.isEmpty();
            }

            public boolean contains(Object o) {

                return map.containsKey(o);
            }

            public Iterator<K> iterator() {
                // TODO Auto-generated method stub
                return new MyKeyIterator<K>(this.map.getDb(),this.map.name);
            }

            public Object[] toArray() {
                // TODO Auto-generated method stub
                throw new UnsupportedOperationException();
            }

            public <T> T[] toArray(T[] a) {
                // TODO Auto-generated method stub
                throw new UnsupportedOperationException();
            }

            public boolean add(K e) {
                // TODO Auto-generated method stub
                throw new UnsupportedOperationException();
            }

            public boolean remove(Object o) {
                // TODO Auto-generated method stub
                this.map.remove(o);
                return true;
            }

            public boolean containsAll(Collection<?> c) {
                // TODO Auto-generated method stub
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
                // TODO Auto-generated method stub
                throw new UnsupportedOperationException();
            }

            public boolean retainAll(Collection<?> c) {
                // TODO Auto-generated method stub
                throw new UnsupportedOperationException();
            }

            public boolean removeAll(Collection<?> c) {
                // TODO Auto-generated method stub
                for (Object o : c) {
                    this.map.remove(o);
                }
                return true;
            }

            public void clear() {
                // TODO Auto-generated method stub
                this.map.clear();
            }

        }

        private final class InnerMapEntrySet<K, V> extends
                AbstractSet<Map.Entry<K, V>> {
            private MyEntryIterator<K, V> iterator = null;
            private Map<K, V> map = null;

            public InnerMapEntrySet(InnerMap<K, V> map) {
                this.iterator = new MyEntryIterator<K, V>(map.getDb(),map.name);
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
                // TODO Auto-generated method stub
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
                // TODO Auto-generated method stub
                if(this.hasNext()){
                    Entry<byte[], byte[]> entry = this.iter.next();
                    String k = new String(entry.getKey()).replaceAll(this.name+'\0', "");
                    return new SimpleEntry((K) Utils.deserialize(k.getBytes()),
                            (V) Utils.deserialize(entry.getValue()));
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
                return (K) Utils.deserialize(k.getBytes());
            }
            throw new NoSuchElementException();
            
        }

        public void remove() {
            // TODO Auto-generated method stub
            this.iter.remove();
        }

    }
    
    
    public static void main(String[] args) throws Exception{
        MapFactory<String,String> mf = new MapFactory("c:/tmp/","test");
        Map<String,String> mf2 = mf.getMap("test1");
        for(int i=0;i<5;i++){
            mf2.put(Integer.toString(i), Integer.toString(i));
        }
        
        Map<String,String> mf3 = mf.getMap("test2");
        for(int i=5;i<10;i++){
            mf3.put(Integer.toString(i), Integer.toString(i));
        }
        
        for(int i=0;i<5;i++){
            System.err.println(mf2.get(Integer.toString(i)));
            System.err.println(mf3.get(Integer.toString(i)));
        }
        
        
        for(int i=5;i<10;i++){
            System.err.println(mf3.get(Integer.toString(i)));
            System.err.println(mf2.get(Integer.toString(i)));
        }
        
        
        
        /*
        for(int i=200;i<300;i++){
            System.err.println(mf2.get(Integer.toString(i)));
        }
        */
        
        /*
        for(Map.Entry<String, String>me:mf2.entrySet()){
            System.err.println(me.getKey() +":"+me.getValue());
        }
        
        for(Map.Entry<String, String>me:mf3.entrySet()){
            System.err.println(me.getKey() +":"+me.getValue());
        }
        */
        mf.close();
        
    }

}
