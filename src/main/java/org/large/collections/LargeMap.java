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
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

public class LargeMap<K, V> implements Map<K, V> {
    /* Configuration of the LargeMap */
    private int cacheSize = 1000;
    private int bloomSize = 20000000;// 10 Million
    private float falsePosTolerance = 0.01f;
    private boolean supportsContainsValue = false;


    /*Caches and Bloom Filters*/
    private Cache<K, V> cache;
    protected Funnel<K> keyFunnel = new Funnel<K>() {
        public void funnel(K obj, PrimitiveSink into) {
            into.putInt(Math.abs(obj.hashCode()));
        }
    };
    protected Funnel<V> valueFunnel = new Funnel<V>() {
        public void funnel(V obj, PrimitiveSink into) {
            into.putInt(Math.abs(obj.hashCode()));
        }
    };
    private transient BloomFilter<K> keyBloomFilter;
    private transient BloomFilter<V> valueBloomFilter;

    /*Actual Data*/
    private LargeDataStore<K,V> store;
    
    
    /*Statistics*/
    private long cacheHitCnt = 0;
    private long keyBloomDiscoveryCnt=0;
    
    private void initializeBloomFilters(){
        keyBloomFilter = BloomFilter.create(keyFunnel, bloomSize,
                falsePosTolerance);
        if(supportsContainsValue){
            valueBloomFilter = BloomFilter.create(valueFunnel, bloomSize,
                    falsePosTolerance);
            
        }
        else{
            valueBloomFilter = null;
        }
        
    }

    public LargeMap() {
        cache = CacheBuilder.newBuilder().maximumSize(cacheSize).build();
        initializeBloomFilters();
    }
    public LargeMap(String name) {
        this();
        store = new LargeDataStore<K,V>(null,name);
        store.initialize();        
    }
    public LargeMap(String basePath,String name) {
        this();
        store = new LargeDataStore<K,V>(basePath,name);
        store.initialize();    
        
    }
    
    public LargeMap(String basePath,String name,boolean reallyLargeMap) {
        this();
        store = new LargeDataStore<K,V>(basePath,name);
        store.setReallyLargeMap(reallyLargeMap);
        store.initialize();          
    }
    
    public int size() {
        return (int)(this.store.getSize());
    }

    public long getSize() {
        return this.store.getSize();             
    }
    
    public boolean isEmpty() {
        return (this.store.getSize()==0);
    }

    /*
     * This can be an expensive operation. Hence it is heuristic. The real
     * meaning is mayContainsKey. The reason being it uses a bloom filter
     */
    public boolean containsKey(Object key) {
        return this.keyBloomFilter.mightContain((K) key);
    }

    public boolean containsValue(Object value) {
        // TODO Auto-generated method stub
        return this.valueBloomFilter.mightContain((V) value);
    }

    public V get(Object key) {
        V val = null;
        val = this.cache.getIfPresent(key);
        if(val!=null){
            cacheHitCnt++;
        }
        else{
            boolean valExists = this.keyBloomFilter.mightContain((K)key);
            if(valExists){
                this.keyBloomDiscoveryCnt++;
                return store.getValue((K)key);
            }
        }
        return val;
    }

    public V put(K key, V value) {
        // TODO Auto-generated method stub
        this.cache.put(key, value);
        this.keyBloomFilter.put(key);
        if(this.supportsContainsValue){
            this.valueBloomFilter.put(value);
        }
        store.putValue(key, value);
        return value;
    }

    public V remove(Object key) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    public void putAll(Map<? extends K, ? extends V> m) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    public void clear() {
        // TODO Auto-generated method stub
        cache.cleanUp();
        this.initializeBloomFilters();
        this.store.reset();
        
    }
  
  
    public Set<K> keySet() {
        throw new UnsupportedOperationException();
    }

    public Collection<V> values() {
        throw new UnsupportedOperationException();
    }

    public Set<java.util.Map.Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException();    
    }
    
    

    public long getCacheHitCnt() {
        return cacheHitCnt;
    }
    public long getKeyBloomDiscoveryCnt() {
        return keyBloomDiscoveryCnt;
    }
    
    private static String getLongString(long l){
        String ls = Long.toString(l);
        StringBuilder sb = new StringBuilder(ls);
        for(int i=0;i<20;i++){
            sb.append(ls);
        }
        return ls.toString();
    }
    public static void main(String[] args){
        Object2ObjectOpenHashMap<String,String> futilsMap = new Object2ObjectOpenHashMap<String,String>();
        System.out.println("Using Fast Utils");
        long total = 3000000;
        long ts = System.currentTimeMillis();
        for(long i = 0;i<total;i++){
            futilsMap.put(Long.toString(i),getLongString(i));
            if(i%1000000==0){
                System.out.println("Checkpoint=="+i+". Elapsed time in ms =="+(System.currentTimeMillis()-ts));
            }
        }
        ts = System.currentTimeMillis();
        for(long i = 0;i<total;i++){
            String val = futilsMap.get(Long.toString(i));
            if(i%1000000==0){
                System.out.println("Sampled, key="+i + ",value="+val+". Elapsed time in ms =="+(System.currentTimeMillis()-ts));
            }
        }
        
        total = 5000000;
        System.out.println("Using Large Map");
        LargeMap<String,String> map = new LargeMap<String,String>("c:/tmp","mymap");
        ts = System.currentTimeMillis();
        for(long i = 0;i<total;i++){
            map.put(Long.toString(i),getLongString(i));
            if(i%100000==0){
                System.out.println("Checkpoint=="+i+". Elapsed time in ms =="+(System.currentTimeMillis()-ts));
            }
        }

        System.out.println("Start Getting");
        ts = System.currentTimeMillis();
        Random r = new Random();
        //r.nextInt(total);
        for(long i = 0;i<total;i++){
            String myKey = Long.toString(r.nextInt((int)total));
            String val = map.get(myKey);
            if(i%100000==0){
                System.out.println("Sampled"+i+", key="+myKey + ",value="+val+". Elapsed time in ms =="+(System.currentTimeMillis()-ts));
            }
            
            
        }
        System.out.println("Cache Hits = " + map.getCacheHitCnt());
        System.out.println("Bloom Hits = " + map.getKeyBloomDiscoveryCnt());
        map.clear();
    }
    
}
