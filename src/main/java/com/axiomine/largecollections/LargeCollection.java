package com.axiomine.largecollections;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import com.google.common.base.Throwables;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

public abstract class LargeCollection implements IDb {
    public static final Random rnd = new Random();
    public static int DEFAULT_CACHE_SIZE = 25;
    public static int DEFAULT_BLOOM_FILTER_SIZE = 10000000;
    public static String DEFAULT_FOLDER = System.getProperty("java.io.tmpdir");
    protected String dbPath;
    protected String dbName;
    protected int cacheSize;
    protected long size;
    protected transient DB db;
    protected transient Options options;
    protected transient File dbFile;

    //private SerializationUtils<K, V> serdeUtils = new SerializationUtils<K, V>();
    protected int bloomFilterSize = DEFAULT_BLOOM_FILTER_SIZE;
    protected  transient Funnel<Integer> myFunnel = null;
    protected BloomFilter bloomFilter = null;
    
    
    protected void initializeBloomFilter(){
        this.myFunnel = new Funnel() {
            public void funnel(Object obj, PrimitiveSink into) {
                into.putInt(obj.hashCode());                  
              }
            };  
        this.bloomFilter = BloomFilter.create(myFunnel, this.bloomFilterSize);
    }
    

    public LargeCollection(){        
        this.initialize();
        this.initializeBloomFilter();   
    }

    public LargeCollection(String dbName){
        this.dbName=dbName;
        this.initialize();
        this.initializeBloomFilter();   
    }
    public LargeCollection(String dbPath,String dbName){
        this.dbPath=dbPath;
        this.dbName = dbName;
        this.initialize();
        this.initializeBloomFilter();   
    }

    public LargeCollection(String dbPath,String dbName,int cacheSize){
        this.dbPath=dbPath;
        this.dbName = dbName;
        this.cacheSize = cacheSize;
        this.initialize();
        this.initializeBloomFilter();   
    }
    
    public LargeCollection(String dbPath,String dbName, int cacheSize,int bloomFilterSize){
        this.dbPath=dbPath;
        this.dbName = dbName;
        this.cacheSize = cacheSize;
        this.bloomFilterSize = bloomFilterSize;
        this.initialize();
        this.initializeBloomFilter();   
    }

    protected void initialize(){
        try {
            options = new Options();
            options.cacheSize(cacheSize * 1048576);
            dbFile = new File(this.dbPath+File.separator+this.dbName);
            if(!dbFile.exists()){
                dbFile.mkdirs();
            }
            db = factory.open(dbFile,options);            
        } catch (Exception ex) {
            Throwables.propagate(ex);
        }
    }
    
    public String getDBPath(){
        return this.dbPath;
    }
    
    protected void serialize(java.io.ObjectOutputStream stream)
            throws IOException {
        System.out.println("Now serializing " + this.dbName);
        stream.writeObject(this.dbPath);
        stream.writeObject(this.dbName);
        stream.writeInt(this.cacheSize);
        stream.writeLong(this.size);
        stream.writeInt(this.bloomFilterSize);
        stream.writeObject(this.bloomFilter);
        this.db.close();

    }

    protected void deserialize(java.io.ObjectInputStream in) throws IOException,
            ClassNotFoundException {
        this.dbPath = (String) in.readObject();
        this.dbName = (String) in.readObject();
        this.cacheSize = in.readInt();
        this.size = in.readLong();
        this.bloomFilterSize = in.readInt();
        this.bloomFilter = (BloomFilter<Integer>) in.readObject();
        this.initialize();
        System.out.println("Now deserialized " + this.dbName);
    }
    
    public DB getDB() {
        return this.db;
    }
    
    public long getLSize(){
        return size;
    }
    public abstract void optimize();
}
