package org.largecollections;

import java.util.Map;
import java.util.Random;
import java.util.Set;

public class TestUtils {
    public static int max = 1000000;
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

    
     public static void write(Map<String, String> map) {
        long ts = System.currentTimeMillis();

        for (int i = 0; i < max; i++) {
            map.put(Integer.toString(i), Integer.toString(i));
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
