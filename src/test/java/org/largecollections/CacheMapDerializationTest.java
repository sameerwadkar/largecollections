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

import static org.junit.Assert.*;
import static org.largecollections.TestUtils.*;

import java.io.File;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import utils.Utils;

public class CacheMapDerializationTest {
    
    
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
