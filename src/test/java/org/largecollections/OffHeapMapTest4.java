package org.largecollections;

import static org.junit.Assert.*;
import static org.largecollections.TestUtils.*;
import java.io.File;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import utils.Utils;

public class OffHeapMapTest4 {
    private static int max = 1000;


    
    @Test
    public void testDelete(){
        Map<String,String>map = (Map<String, String>) Utils.deserialize(new File("c:/tmp/mymap2.ser"));
        Utils.deleteOffHeapMap(map);
        
    }

  
}
