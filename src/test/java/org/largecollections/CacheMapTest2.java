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

import java.io.Closeable;
import java.io.File;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import utils.DBUtils;

public class CacheMapTest2 {
    
    
    @Test
    public void test01SerializeMap() throws Exception{
    
        TestUtils.max=1000;
        CacheMap<String, String> map = new CacheMap<String, String>("c:/tmp/",
                "cacheMap123");
        CacheMap<String, String> map2 = new CacheMap<String, String>("c:/tmp/",
                "cacheMap456");

        CacheMap<String, String> map3  = new CacheMap<String, String>("c:/tmp/",
                "cacheMap789");
        write(map);
        ((Closeable) map).close();



        write(map2);
        ((Closeable) map2).close();


        write(map3);
        ((Closeable) map3).close();
    }
    

    
 
  
}
