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

import java.io.File;

import org.junit.Test;

import utils.FileSerDeUtils;

public class LargeCacheMapTest {
    
    
    @Test
    public void test01SerializeMap() throws Exception{
    
        TestUtils.max=1000;
        LargeCacheMap<String, String> map = new LargeCacheMap<String, String>("c:/tmp/",
                "lcacheMap");

        write(map);
        TestUtils.read(map);
        TestUtils.readEntrySet(map);
        TestUtils.readKeySet(map);

        FileSerDeUtils.serializeToFile(map,new File("c:/tmp/mymap.ser"));
    }
    

    
 
  
}
