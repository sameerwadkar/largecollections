package org.largecollections;

import static org.junit.Assert.*;
import static org.largecollections.TestUtils.*;
import java.io.File;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import utils.Utils;

public class OffHeapMapTest1 {
    private static int max = 1000;
    
    @Test
    public void test01SerializeMap() {

        Map<String, String> map = new OffHeapMap<String, String>("c:/tmp/",
                "bigsynapse");

        write(map);
        read(map);
        readEntrySet(map);
        Utils.serialize(map,new File("c:/tmp/mymap.ser"));
    }
    

    
 
  
}
