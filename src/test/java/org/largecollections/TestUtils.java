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

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class TestUtils {
    public static int max = 1000;
    public static void writeList(List<String> l){
        for (int i = 0; i < max; i++) {
            l.add(Integer.toString(i));
        }
    }
    
    public static void read(Map<String, String> map) {
        Random rnd = new Random();
        Long ts = System.currentTimeMillis();
        for (int i = 0; i < max; i++) {
            String k = Integer.toString(rnd.nextInt(max));
            String v = map.get(k);
            /*
            if (i % (max / 10) == 0) {
                System.out.println(k + "=" + v);
            }
            */
            
        }
        System.err.println("Time to randomly read a  " + max + " rows "
                + (System.currentTimeMillis() - ts));
    }

    public static void read(Set<String> set) {
        Random rnd = new Random();
        Long ts = System.currentTimeMillis();
        for (String s:set) {
            //System.err.println("Set:"+s);
        }
        System.err.println("Time to randomly read a  " + max + " rows "
                + (System.currentTimeMillis() - ts));
    }
    
    public static void quickRead(Set<String> set) {
        Random rnd = new Random();
        Long ts = System.currentTimeMillis();
        System.err.println("Read First Element");
        for (String s:set) {
            System.err.println("Quick Read Value:"+s);
            break;
        }
        
    }
    
     public static void write(Map<String, String> map) {
        long ts = System.currentTimeMillis();

        for (int i = 0; i < max; i++) {
            map.put(Integer.toString(i), Integer.toString(i));
        }

        System.err.println("Time to insert a  " + max + " rows "
                + (System.currentTimeMillis() - ts));
    }
     
     public static void write(Set<String> set) {
         long ts = System.currentTimeMillis();

         for (int i = 0; i < max; i++) {
             set.add(Integer.toString(i));
            
         }

         System.err.println("Time to insert a  " + max + " rows "
                 + (System.currentTimeMillis() - ts));
     }

     public static void readEntrySet(Map<String, String> map) {
        long ts = System.currentTimeMillis();

        Set<Map.Entry<String, String>> set = map.entrySet();
        int i = 0;
        for (Map.Entry<String, String> e : set) {
           // if (i % (max / 10) == 0)
           //     System.err.println(e.getKey() + "=" + e.getValue());
            i++;
        }
        System.err.println("Time to sequentially read an EntrySet  " + max + " rows "
                + (System.currentTimeMillis() - ts));
    }
    
     public static void readKeySet(Map<String, String> map) {
        long ts = System.currentTimeMillis();

        Set<String> set = map.keySet();
        int i = 0;
        for (String e : set) {
            //map.get(e);
            //if (i % (max / 10) == 0)
                map.get(e);
            i++;
        }
        System.err.println("Time to read KeySet a  " + max + " rows "
                + (System.currentTimeMillis() - ts));
    }
}
