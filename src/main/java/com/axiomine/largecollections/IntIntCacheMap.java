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
package com.axiomine.largecollections;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.iq80.leveldb.WriteBatch;

import com.axiomine.largecollections.functions.IntegerDeSerFunction;
import com.axiomine.largecollections.functions.IntegerSerFunction;
import com.google.common.base.Throwables;



public class IntIntCacheMap extends LargeCollection implements Map<Integer,Integer>, Serializable, Closeable {
    public static final long serialVersionUID = 2l;
    private transient IntegerSerFunction keySerFunc = new IntegerSerFunction();
    private transient IntegerSerFunction valSerFunc = new IntegerSerFunction();
    private transient IntegerDeSerFunction keyDeSerFunc = new IntegerDeSerFunction();
    private transient IntegerDeSerFunction valDeSerFunc = new IntegerDeSerFunction();
    public IntIntCacheMap(){        
        super();
    }

    public IntIntCacheMap(String dbName){
        super(dbName);
    }
    public IntIntCacheMap(String dbPath,String dbName){
        super(dbPath,dbName);
    }

    public IntIntCacheMap(String dbPath,String dbName,int cacheSize){
        super(dbPath,dbName,cacheSize);
    }
    
    public IntIntCacheMap(String dbPath,String dbName, int cacheSize,int bloomFilterSize){
        super(dbPath,dbName,cacheSize,bloomFilterSize);
    }
    

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
    
    public boolean containsKey(Object key) {
        byte[] valBytes = null;
        if(key!=null){
            Integer ki = (Integer)key;
            int kint = ki.intValue();        
            if (this.bloomFilter.mightContain(ki)) {
                byte[] keyBytes = keySerFunc.apply(kint);
                valBytes = db.get(keyBytes);
            }
        }
        return valBytes != null;
    }

    public boolean containsValue(Object value) {
        //Will be very slow unless a seperate DB is maintained with values as keys
        throw new UnsupportedOperationException();

    }

    public Integer get(Object key) {
        byte[] vbytes = null;
        if (key == null) {
            return null;
        } 
        Integer ki = (Integer)key;
        int kint = ki.intValue();        
        if (bloomFilter.mightContain((Integer) key)) {
            vbytes = db.get(keySerFunc.apply(kint));
            if (vbytes == null) {
                return null;
            } else {
                return valDeSerFunc.apply(vbytes);
            }
        } else {
            return null;
        }

    }

    public int size() {
        return (int) size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    /*Putting null values is not allowed for this map*/
    public Integer put(Integer key, Integer value) {
        if(key==null)
            return null;
        if(value==null)//Do not add null key or value
            return null;
        byte[] fullKeyArr = keySerFunc.apply(key);
        byte[] fullValArr = valSerFunc.apply(value);
        if (!this.containsKey(key)) {            
            bloomFilter.put(key);
            db.put(fullKeyArr, fullValArr);
            size++;
        } else {
            db.put(fullKeyArr,fullValArr);
        }
        return value;
    }

    public Integer remove(Object key) {
        Integer v = null;
        if(key==null)
            return v;               
        if (this.size > 0  && this.bloomFilter.mightContain((Integer)key)) {
            v = this.get(key);
        }

        if (v != null) {
            byte[] fullKeyArr = keySerFunc.apply((Integer)key);
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
                byte[] keyArr = keySerFunc.apply(e.getKey());
                Integer v = null;
                if (this.size > 0  && this.bloomFilter.mightContain(e.getKey())) {
                    v = this.get(e.getKey());
                }
                if (v == null) {  
                    bloomFilter.put(e.getKey());
                    this.size++;
                }
                batch.put(keyArr,valSerFunc.apply(e.getValue()));
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
        this.serialize(stream);

    }

    private void readObject(java.io.ObjectInputStream in) throws IOException,
            ClassNotFoundException {
        this.deserialize(in);
        keySerFunc = new IntegerSerFunction();
        valSerFunc = new IntegerSerFunction();
        keyDeSerFunc = new IntegerDeSerFunction();
        valDeSerFunc = new IntegerDeSerFunction();
    }

    public void clear() {
        this.initialize();
        this.initializeBloomFilter();
    }

    public Set<Integer> keySet() {
        return new MapKeySet<Integer>(this,keyDeSerFunc);
    }

    public Collection<Integer> values() {
        return new ValueCollection<Integer>(this,this.getDB(),this.valDeSerFunc);
    }

    public Set<java.util.Map.Entry<Integer, Integer>> entrySet() {
        return new MapEntrySet<Integer, Integer>(this,this.keyDeSerFunc,this.valDeSerFunc);
    }

    public void close() throws IOException {
        try {
            this.initializeBloomFilter();
            this.db.close();
            factory.destroy(this.dbFile, this.options);
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }

 
 
}
