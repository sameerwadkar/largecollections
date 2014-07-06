package org.largecollections;

import static org.junit.Assert.*;

import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

public class ListFactoryTest {

    @Test
    public void testGetInstance() {
        ListFactory<String> lstFactory = new ListFactory<String>("c:/tmp","lstFactory",25);
        List<String> lst1 = lstFactory.getList("l1");
        List<String> lst2 = lstFactory.getList("l2");
        for(int i=0;i<5;i++){
            lst1.add(Integer.toString(i));
        }
        for(int i=0;i<5;i++){
            Assert.assertEquals(Integer.toString(i), lst1.get(i));
        }
        for(int i=5;i<10;i++){
            lst2.add(Integer.toString(i));
        }
        for(int i=5;i<10;i++){
            Assert.assertEquals(Integer.toString(i), lst2.get(i-5));
        }
        
        Assert.assertEquals(5, lst1.size());
        Assert.assertEquals(5, lst2.size());
        lstFactory.close();
    }

}
