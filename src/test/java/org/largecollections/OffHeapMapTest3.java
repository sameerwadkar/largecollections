package org.largecollections;

import static org.junit.Assert.*;
import static org.largecollections.TestUtils.*;
import java.io.File;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import utils.Utils;

public class OffHeapMapTest3 {
    private static int max = 1000;


    
    @Test
    public void test03DerializeMap(){
        Map<String,String>map = (Map<String, String>) Utils.deserialize(new File("c:/tmp/mymap2.ser"));
        System.out.println("Deserialize=" + map.size());
        read(map);
        readKeySet(map);
        System.out.println(map.get("X"));
    }

  
}
