/*
 * Copyright 2014 Sameer Wadkar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.largecollections;

import static org.largecollections.TestUtils.write;

import org.junit.Test;
import  org.junit.Assert;

public class CacheSetTest {
    
    
    @Test
    public void testBasic() throws Exception{
        
        TestUtils.max=50;
        long ts = System.currentTimeMillis();
        
        CacheSet<String> set = new CacheSet<String>("c:/tmp/",
                "cacheSet");

        TestUtils.write(set);
        System.err.println(System.currentTimeMillis()-ts);
        
        TestUtils.read(set);
        Assert.assertEquals(TestUtils.max, set.size());
        set.remove(Integer.toString(Integer.MAX_VALUE));
        Assert.assertEquals(TestUtils.max, set.size());
        set.remove("1");
        Assert.assertEquals((TestUtils.max-1), set.size());
        set.clear();
        Assert.assertEquals(0, set.size());
        //System.err.println("After Clear Size=" + set.size());
        TestUtils.quickRead(set);//Should not return anything;
        
        set.close();
       
    }
    

    
 
  
}
