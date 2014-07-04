package org.largecollections;

import static org.junit.Assert.*;

import java.util.Map;

import org.junit.Test;

public class MapFactoryTest {

   
    public void test() {
        MapFactory<String,String> factory = new MapFactory("/tmp/","test");
        Map<String,String> map1 = factory.getMap("test1");
        for(int i=0;i<5;i++){
            map1.put(Integer.toString(i), Integer.toString(i));
        }
        
        Map<String,String> map2 = factory.getMap("test2");
        for(int i=5;i<10;i++){
            map2.put(Integer.toString(i), Integer.toString(i));
        }
        
        map2.remove(Integer.toString(7));
        for(int i=0;i<5;i++){
            System.err.println("Map 1 Access: "+ map1.get(Integer.toString(i)));
            System.err.println("Map 2 Access: "+ map2.get(Integer.toString(i)));
        }
        
        
        for(int i=5;i<10;i++){
            System.err.println("Map 1 Access: "+ map1.get(Integer.toString(i)));
            System.err.println("Map 2 Access: "+ map2.get(Integer.toString(i)));
        }
    }

}
