package org.largecollections;

import static org.junit.Assert.*;
import static org.largecollections.TestUtils.*;
import java.io.File;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import utils.Utils;

public class OffHeapMapTest6 {
    public static int max = 1000000;
    
    @Test
    public void test01SerializeMap() {

        Map<String, String> map = new OffHeapMap<String, String>("c:/tmp/",
                "bigsynapse");

        write(map);
        TestUtils.read(map);
        TestUtils.readEntrySet(map);
        TestUtils.readKeySet(map);
        Utils.deleteOffHeapMap(map);
        map = new OffHeapMap<String, String>("c:/tmp/",
                "bigsynapse");
        write(map);
        Utils.deleteOffHeapMap(map);
    }
    

    
 
  
}
