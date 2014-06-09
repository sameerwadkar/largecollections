package org.largecollections;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import static org.largecollections.TestUtils.*;
import org.junit.Test;

import utils.Utils;

public class OffHeapMapTest2 {
    private static int max = 1000;
    
 
    
    @Test
    public void test02SerializeMap() {
        Map<String,String>map =  ( Map<String,String>)Utils.deserialize(new File("c:/tmp/mymap.ser"));
        read(map);
        readKeySet(map);
        map.put("X", "Y");
        Utils.serialize(map,new File("c:/tmp/mymap2.ser"));
    }
    


}
