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

import java.io.IOException;
import java.util.Map;

import org.iq80.leveldb.WriteBatch;

import utils.Utils;

import com.google.common.base.Throwables;

public class LargeCacheMap<K, V> extends OffHeapMap<K,V>{
    public  static final long serialVersionUID = 2l;
    private int size=0;
    private long longSize=0;
    public LargeCacheMap(String folder, String name, int cacheSize,
            String comparatorCls) {
       super(folder,name,cacheSize,comparatorCls);
    }

    public LargeCacheMap(String folder, String name, int cacheSize) {
        super(folder,name,cacheSize,null);
    }

    public LargeCacheMap(String folder, String name) {
        super(folder,name,LargeCacheMap.DEFAULT_CACHE_SIZE, null);
    }

    public LargeCacheMap(String folder) {
        super(folder,LargeCacheMap.DEFAULT_NAME,LargeCacheMap.DEFAULT_CACHE_SIZE, null);
    }

    public LargeCacheMap() {
        this(LargeCacheMap.DEFAULT_FOLDER, LargeCacheMap.DEFAULT_NAME,
                LargeCacheMap.DEFAULT_CACHE_SIZE, null);
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
            db.put(Utils.serialize(key), Utils.serialize(value));
            size++;
            longSize++;
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
        stream.writeObject(this.dbComparatorCls);
        stream.writeInt(this.size);
        stream.writeLong(this.longSize);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException,
            ClassNotFoundException {
        this.folder = (String) in.readObject();
        this.name = (String) in.readObject();
        this.cacheSize = in.readInt();
        this.dbComparatorCls = (String) in.readObject();
        this.size = in.readInt();
        this.longSize = in.readLong();
        this.db = this.createDB(this.folder, this.name, this.cacheSize,
                this.dbComparatorCls);
    }


}
