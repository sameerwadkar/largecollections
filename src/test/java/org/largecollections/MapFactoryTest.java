package org.largecollections;

import static org.junit.Assert.*;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import utils.FileSerDeUtils;

public class MapFactoryTest {
    MapFactory<String,String> factory = null;
    private String factoryFolder = "c:/tmp/";
    private String factoryName = "factory";
    private String name1 = "test1";
    private String name2 = "test2";
    private File serializedFile = new File("c:/tmp/1.ser");
    @Before
    public void setUp(){
        if(serializedFile.exists()){
            serializedFile.delete();
        }
        this.factory = new MapFactory(factoryFolder,factoryName);
    }
    
    @After
    public void tearDown(){
        if(serializedFile.exists()){
            serializedFile.delete();
        }
        this.factory.close();
    }
    
   
   @Test
    public void testBasicFunctionality() {
        
        Map<String,String> map1 = factory.getMap(name1);
        for(int i=0;i<5;i++){
            map1.put(Integer.toString(i), Integer.toString(i));
        }
        
        Map<String,String> map2 = factory.getMap(name2);
        for(int i=5;i<10;i++){
            map2.put(Integer.toString(i), Integer.toString(i));
        }
        Assert.assertEquals(5, map2.size());
        map2.remove(Integer.toString(7));
        Assert.assertEquals(4, map2.size());
        
        for(int i=0;i<5;i++){
            Assert.assertEquals(Integer.toString(i), map1.get(Integer.toString(i)));
            Assert.assertNull(map2.get(Integer.toString(i)));
        }
        
        
        for(int i=5;i<10;i++){
            if(i!=7){
                Assert.assertEquals(Integer.toString(i), map2.get(Integer.toString(i)));
            }
            else{
                Assert.assertNull(map2.get(Integer.toString(i)));
            }
            
            Assert.assertNull(map1.get(Integer.toString(i)));
        }
        map2.clear();
        System.err.println("Map2 Size="+map2.size());
        System.err.println("Map1 Size="+map1.size());
        Assert.assertEquals(0, map2.size());
        Assert.assertEquals(5, map1.size());
    }
   
   
   
   @Test
   public void testSerialization() {
  
       Map<String,String> map1 = factory.getMap(name1);
       for(int i=0;i<5;i++){
           map1.put(Integer.toString(i), Integer.toString(i));
       }
       
       Map<String,String> map2 = factory.getMap(name2);
       for(int i=5;i<10;i++){
           map2.put(Integer.toString(i), Integer.toString(i));
       }
       Assert.assertEquals(5, map2.size());
       map2.remove(Integer.toString(7));
       Assert.assertEquals(4, map2.size());
       
       for(int i=0;i<5;i++){
           Assert.assertEquals(Integer.toString(i), map1.get(Integer.toString(i)));
           Assert.assertNull(map2.get(Integer.toString(i)));
       }
       
       
       for(int i=5;i<10;i++){
           if(i!=7){
               Assert.assertEquals(Integer.toString(i), map2.get(Integer.toString(i)));
           }
           else{
               Assert.assertNull(map2.get(Integer.toString(i)));
           }
           
           Assert.assertNull(map1.get(Integer.toString(i)));
       }
       
       Map<String,String> map21 = factory.getMap(name1);
       Map<String,String> map22 = factory.getMap(name2);
       Assert.assertEquals(5, map21.size());
      
       Assert.assertEquals(4, map22.size());
       //factory.reindexBloomFilter();
       
       FileSerDeUtils.serializeToFile(factory, serializedFile);
       
       factory = (MapFactory<String,String>)FileSerDeUtils.deserializeFromFile(serializedFile);
       map21 = factory.getMap(name1);
       map22 = factory.getMap(name2);
       Assert.assertEquals(5, map21.size());      
       Assert.assertEquals(4, map22.size());
   }
   @Test
   public void testPutAll() {
       Map<String,String> map1 = factory.getMap(name1);
       
       Map<String,String> tempMap1 = new HashMap<String,String>();
       for(int i=0;i<5;i++){
           tempMap1.put(Integer.toString(i), Integer.toString(i));
       }  
       map1.putAll(tempMap1);
       Assert.assertEquals(5, map1.size());
   }
   
   
   @Test
   public void testClear() {
       Map<String,String> map1 = factory.getMap(name1);
       
       Map<String,String> tempMap1 = new HashMap<String,String>();
       for(int i=0;i<5;i++){
           tempMap1.put(Integer.toString(i), Integer.toString(i));
       }  
       map1.putAll(tempMap1);
       Assert.assertEquals(5, map1.size());


       map1.clear();
       Assert.assertEquals(0, map1.size());
   }
   
   @Test
   public void testKeySet() {
       Map<String,String> map1 = factory.getMap(name1);
       
       Map<String,String> tempMap1 = new HashMap<String,String>();
       for(int i=0;i<5;i++){
           tempMap1.put(Integer.toString(i), Integer.toString(i));
       }  
       map1.putAll(tempMap1);
       System.err.println(map1.get("4"));
       Set<String> keys = map1.keySet();
       for (String k : keys) {
           System.err.println(k);
       }
   }
   @Test
   public void testEntrySet() {
       Map<String,String> map1 = factory.getMap(name1);
       
       Map<String,String> tempMap1 = new HashMap<String,String>();
       for(int i=0;i<5;i++){
           tempMap1.put(Integer.toString(i), Integer.toString(i));
       }  
       map1.putAll(tempMap1);
       System.err.println(map1.get("4"));
       //Set<String> keys = map1.entrySet();
       for (Map.Entry<String, String> e: map1.entrySet()) {
           System.err.println("Entry Set Key:"+e.getKey() + ", Entry Set Value:"+e.getValue());
       }
   }
   /*
   @Test
   public void testKeySetForPerformance() {
       Map<String,String> map1 = factory.getMap(name1);
       
       Map<String,String> tempMap1 = new HashMap<String,String>();
       for(int i=0;i<1000000;i++){
           tempMap1.put(Integer.toString(i), Integer.toString(i));
       }  
       map1.putAll(tempMap1);
       System.err.println(map1.get("50000"));
       Set<String> keys = map1.keySet();
       for (String k : keys) {
           //System.err.println(k);
       }
   }
   */
    
}
