package org.largecollections;

import static org.junit.Assert.*;
import static org.largecollections.TestUtils.*;

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import junit.framework.Assert;

import org.junit.Test;

import utils.Utils;

public class OffHeapMapTest5 {
    private static int max = 1000;


    
    @Test
    public void testIsEmpty() throws Exception{
        OffHeapMap<String, String> map = new OffHeapMap<String, String>("c:/tmp/",
                "bigsynapse");

        Assert.assertEquals(true,map.isEmpty());
        
        map.put("1", "1");
        Iterator it = map.keySet().iterator();
        while(it.hasNext()){
            System.err.println(it.next());
        }
        it = map.keySet().iterator();
        while(it.hasNext()){
            System.err.println(it.next());
        }
        Assert.assertEquals(false,map.isEmpty());
        map.remove("1");
        Assert.assertEquals(true,map.isEmpty());
        map.close();
    }

  
}
