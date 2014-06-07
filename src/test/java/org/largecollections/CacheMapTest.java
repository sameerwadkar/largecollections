package org.largecollections;

import static org.junit.Assert.*;
import static org.largecollections.TestUtils.*;
import java.io.File;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import utils.Utils;

public class CacheMapTest {
    
    
    @Test
    public void test01SerializeMap() throws Exception{
    
        TestUtils.max=1000000;
        OffHeapMap<String, String> map = new OffHeapMap<String, String>("c:/tmp/",
                "cacheMap");

        write(map);
        TestUtils.read(map);
        TestUtils.readEntrySet(map);
        TestUtils.readKeySet(map);
        
        map.remove(Integer.toString(Integer.MAX_VALUE));
        System.err.println("Size=" + map.size());
        map.remove("1");
        System.err.println("Size=" + map.size());
        map.put("1","1");
        map.put("1","1");
        map.put("1","1");
        System.err.println("Size=" + map.size());
        map.close();
    }
    

    
 
  
}
