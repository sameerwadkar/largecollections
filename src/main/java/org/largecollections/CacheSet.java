package org.largecollections;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;

import utils.Utils;

import com.google.common.base.Throwables;

public  class CacheSet<K> implements Set<K>,Closeable {
    public  static final long serialVersionUID = 10l;
    private final static Random rnd = new Random();
    

    protected String folder = Constants.DEFAULT_FOLDER;
    protected int cacheSize = Constants.DEFAULT_CACHE_SIZE;
    protected String name = null;    
    private int size=0;    
    protected transient DB db;
    protected transient Options options;
    protected transient File dbFile;
    
    public CacheSet(String folder, String name, int cacheSize,
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

    public CacheSet(String folder, String name, int cacheSize) {
        this(folder, name, cacheSize, null);
    }

    public CacheSet(String folder, String name) {
        this(folder, name, Constants.DEFAULT_CACHE_SIZE, null);
    }

    public CacheSet(String folder) {       
        this(folder,"TMP" + rnd.nextInt(1000000), Constants.DEFAULT_CACHE_SIZE,
                null);
    }

    public CacheSet() {
        this(Constants.DEFAULT_FOLDER, "TMP" + rnd.nextInt(1000000),
                Constants.DEFAULT_CACHE_SIZE, null);
    }

    
    public int size() {
        return size;
    }

   
    public boolean isEmpty() {
        return (this.size==0);
    }

   
    public boolean contains(Object key) {
        return db.get(Utils.serialize(key)) != null;
    }

    public Iterator<K> iterator() {
        return new ValueIterator<K>(this.db);
    }

    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException();
    }

    public boolean add(K e) {
        byte[] v = this.db.get(Utils.serialize(e.toString()));
        if(v==null){
            db.put(e.toString().getBytes(), Utils.serialize(e));
            size++;
            return true;
        }
        return false;
    }

    
    public boolean remove(Object e) {
        byte[] v = this.db.get(e.toString().getBytes());
        if(v!=null){
            this.delete(e.toString());
            return true;
        }
        
        return false;
    }

    private void delete(String s){
        db.delete(s.getBytes());
        size--;
    }
  
  
    public void clear() {
        //Get all keys and delete all
        DBIterator iter = this.db.iterator();
        iter.seekToFirst();
        while(iter.hasNext()){
            String k = new String(iter.next().getKey());
            this.delete(k);
            
            //iter.remove();
            //this.size--;
            System.err.println("current size"+size);
        }
    }

    public boolean containsAll(Collection<?> c) {
        boolean ret = true;
        Iterator i = c.iterator();
        while(i.hasNext()){
            K v= (K) i.next();
            byte[] key =  v.toString().getBytes();
            byte[] bts = this.db.get(key);
            if(bts!=null){
                K vv = (K)Utils.deserialize(bts);
                if(!vv.equals(v)){
                    ret=false;
                    break;
                }
            }
            else{
                ret=false;
                break;
            }
        }
        return ret;
    }


    public boolean addAll(Collection<? extends K> c) {
        Iterator i = c.iterator();
        int stSize = this.size;
        while(i.hasNext()){
            K k = (K)i.next();
            this.add(k);
        }
        if(this.size>stSize){
            return true;
        }
        else{
            return false;    
        }
        
    }

    public boolean retainAll(Collection<?> c) {
        boolean changed = false;
        Iterator i = c.iterator();
        int stSize = this.size;
        while(i.hasNext()){
            K k = (K)i.next();
            if(!this.contains(k)){
                this.remove(k);
                changed=true;
            }
        }
        return changed;
    }

    public boolean removeAll(Collection<?> c) {
        boolean changed = false;
        Iterator i = c.iterator();
        int stSize = this.size;
        while(i.hasNext()){
            K k = (K)i.next();
            if(this.contains(k)){
                this.remove(k);
                changed=true;
            }
        }
        return changed;
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
