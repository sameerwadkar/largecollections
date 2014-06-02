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
package org.large.collections;

import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import utils.Utils;

import com.google.common.base.Throwables;
import com.higherfrequencytrading.chronicle.Excerpt;
import com.higherfrequencytrading.chronicle.impl.IndexedChronicle;
import com.higherfrequencytrading.chronicle.tools.ChronicleTools;

public class LargeDataStore<K, V> {
    /* configuration */
    public static final String TEMP_MAP="test";
    private String path = null;
    private String basePath=System.getProperty("java.io.tmpdir");
    private boolean deleteOnExit = true;
    private boolean reallyLargeMap = false;

    /* Actual Data */
    private Object2LongOpenHashMap<K> keys = new Object2LongOpenHashMap<K>();
    private IndexedChronicle chronicle;
    private Excerpt excerpt = null;
    final int[] consolidates = new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
            12, 13, 14, 15, 16 };

    /* State Variables */
    private long currentIndex = -1;
    private long size = 0;
    private boolean overwriteAllowed = true;

    /*Chronicle for Mapping Keys To Indexes Maps*/
    private int noOfSplits = 97;//Keep this as a prime number
    private List<IndexedChronicle> keyMapChronicles = new ArrayList<IndexedChronicle>();
    private List<Excerpt> keyMapExcerpts = new ArrayList<Excerpt>();
    private int sizeOfKeyMap = 100;
    private List<Object2LongOpenHashMap<K>> keyMap = new ArrayList<Object2LongOpenHashMap<K>>();
    
    public void initialize() {
        try{
            if (deleteOnExit)
                ChronicleTools.deleteOnExit(basePath);
            chronicle = new IndexedChronicle(basePath);
            this.excerpt = chronicle.createExcerpt();
            if(this.reallyLargeMap)
                this.initializeIndexes();
            currentIndex=-1;
            size = 0;
        }
        catch(Exception ex){
            Throwables.propagate(ex);
        }        
    }
    
    
    public void initializeIndexes() {
        try{
            for(int i=0;i<this.noOfSplits;i++){
                String fName = this.basePath+this.path+File.separator+"index"+i;
                if (deleteOnExit)
                    ChronicleTools.deleteOnExit(fName);
                IndexedChronicle keyMapChronicle = new IndexedChronicle(fName);
                keyMapChronicles.add(chronicle);
                Excerpt keyMapExcerpt = chronicle.createExcerpt();
                keyMapExcerpts.add(keyMapExcerpt);
            }
        }
        catch(Exception ex){
            Throwables.propagate(ex);
        }        
    }
    
    public LargeDataStore(String basePath,String path) {
        if(basePath!=null){
            this.basePath = basePath;
        }
        if(path!=null){
            this.path = path;
        }
        this.basePath = this.basePath+ File.separator + this.path;
    }

    
    
    
    public String getPath() {
        return path;
    }
    public void setPath(String path) {
        this.path = path;
    }
    public String getBasePath() {
        return basePath;
    }
    public void setBasePath(String basePath) {
        this.basePath = basePath;
    }
    public long getSize() {
        return size;
    }
    
    

    public boolean isReallyLargeMap() {
        return reallyLargeMap;
    }


    public void setReallyLargeMap(boolean reallyLargeMap) {
        this.reallyLargeMap = reallyLargeMap;
    }


    private void makePersistent(V value) {
        this.size++;
        this.currentIndex++;

        byte[] valBytes = Utils.serialize(value);
        excerpt.startExcerpt(Utils.sizeof(int.class) + valBytes.length + Utils.sizeof(int.class)
                * consolidates.length);
        excerpt.writeInt(valBytes.length);
        excerpt.write(valBytes);
        excerpt.writeLong(System.nanoTime());
        for (final int consolidate : consolidates) {
            excerpt.writeStopBit(consolidate);
        }
        excerpt.finish();

    }

    public V getValue(K key) {
        V value = null;
        long index = this.keys.get(key);
        if (index >= 0 && this.excerpt.index(index)) {
            // byte[] b
            int length = this.excerpt.readInt();
            byte[] bts = new byte[length];
            this.excerpt.read(bts);
            value = (V) Utils.deserialize(bts);
        }
        return value;
    }

    /* Overwritting of keys is allowed */
    public V putValue(K key, V value) {
        if (keys.containsKey(key)) {
            if (overwriteAllowed) {
                // data.put(key, Utils.serialize(value));
                this.makePersistent(value);
                keys.put(key, this.currentIndex);
            }           
        }
        else{
            this.makePersistent(value);
            keys.put(key, this.currentIndex);
        }

        // Just to respect the interface. Cannot be certain of this if
        // overwriteAllowed is not true
        return value;
    }
    
    public void reset(){
        try{           
            this.chronicle.close();
            this.initialize();

        }
        catch(Exception ex){
            Throwables.propagate(ex);
        }
    }
}
