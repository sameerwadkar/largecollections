package org.largecollections;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Map;

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

       FileSerDeUtils.serializeToFile(factory, serializedFile);
       
       factory = (MapFactory<String,String>)FileSerDeUtils.deserializeFromFile(serializedFile);
       Map<String,String> map21 = factory.getMap(name1);
       Map<String,String> map22 = factory.getMap(name2);
       Assert.assertEquals(5, map21.size());
      
       Assert.assertEquals(4, map22.size());
       
   }

}
