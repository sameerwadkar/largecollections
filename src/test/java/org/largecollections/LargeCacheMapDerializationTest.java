package org.largecollections;

import static org.junit.Assert.*;
import static org.largecollections.TestUtils.*;

import java.io.File;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import utils.Utils;

public class LargeCacheMapDerializationTest {
    
    
    @Test
    public void test01DeSerializeMap() throws Exception{    
        Map<String,String>map = (Map<String, String>) Utils.deserialize(new File("c:/tmp/mymap.ser"));
        System.out.println("Deserialize=" + map.size());
        read(map);
        readKeySet(map);
        map.put("X", "Y");
        System.out.println(map.get("1"));
    }
    

    
 
  
}
