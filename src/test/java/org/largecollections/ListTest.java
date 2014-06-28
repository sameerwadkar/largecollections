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
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import utils.DBUtils;

public class ListTest {
    
    
    @Test
    public void test01List() throws Exception{    
        System.setProperty("java.io.tmpdir","c:/tmp");
        TestUtils.max=1000;
        WriteOnceReadManyArrayList<String> l = new WriteOnceReadManyArrayList<String>();

        TestUtils.writeList(l);
        
        Iterator<String> ls = l.iterator();
        while(ls.hasNext()){
            System.out.println(ls.next());
        }
        DBUtils.serialize(l,new File("c:/tmp/lst.ser"));
    }  
}
