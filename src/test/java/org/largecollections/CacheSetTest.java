package org.largecollections;

import static org.junit.Assert.*;
import static org.largecollections.TestUtils.*;
import java.io.File;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import utils.Utils;

public class CacheSetTest {
    
    
    @Test
    public void testBasic() throws Exception{
        
        TestUtils.max=1000;
        
        CacheSet<String> set = new CacheSet<String>("c:/tmp/",
                "cacheSet");

        write(set);
        TestUtils.read(set);
        
        set.remove(Integer.toString(Integer.MAX_VALUE));
        System.err.println("Size=" + set.size());
        set.remove("1");
        System.err.println("Size=" + set.size());
        set.clear();
        System.err.println("After Clear Size=" + set.size());
        TestUtils.quickRead(set);//Should not return anything;
        set.close();
       
    }
    

    
 
  
}
