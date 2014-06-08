package org.largecollections;

import static org.junit.Assert.*;
import static org.largecollections.TestUtils.*;

import java.io.Closeable;
import java.io.File;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import utils.Utils;

public class CacheMapTest2 {
    
    
    @Test
    public void test01SerializeMap() throws Exception{
    
        TestUtils.max=1000;
        CacheMap<String, String> map = new CacheMap<String, String>("c:/tmp/",
                "cacheMap123");
        CacheMap<String, String> map2 = new CacheMap<String, String>("c:/tmp/",
                "cacheMap456");

        CacheMap<String, String> map3  = new CacheMap<String, String>("c:/tmp/",
                "cacheMap789");
        write(map);
        ((Closeable) map).close();



        write(map2);
        ((Closeable) map2).close();


        write(map3);
        ((Closeable) map3).close();
    }
    

    
 
  
}
