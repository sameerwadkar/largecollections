package org.largecollections;

import static org.junit.Assert.*;
import static org.largecollections.TestUtils.*;

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import utils.Utils;

public class HashMapTest {
    
    
    @Test
    public void test01SerializeMap() throws Exception{
    
        TestUtils.max=1000;
        FactoryHashMap<String, String> map = new FactoryHashMap<String, String>("c:/tmp/",
                "cacheMap");

        write(map);
        Iterator<String> iter1 = map.keySet().iterator();
        Iterator<String> iter2 = map.keySet().iterator();
        
     
        
        System.out.println(iter1.next());
        System.out.println(iter1.next());
        System.out.println(iter2.next());
        System.out.println(iter2.next());
        System.out.println(iter1.next());
        System.out.println(iter1.next());
        System.out.println(iter2.next());
        
        map.close();
        
    }
    

    
 
  
}
