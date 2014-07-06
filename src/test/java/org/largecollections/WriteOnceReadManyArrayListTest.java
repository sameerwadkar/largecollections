package org.largecollections;

import static org.junit.Assert.*;

import java.io.File;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class WriteOnceReadManyArrayListTest {
    WriteOnceReadManyArrayList<String> lst = null;
    private String folder = "c:/tmp/";
    private String name = "lst";

    private File serializedFile = new File("c:/tmp/lst.ser");
    @Before
    public void setUp(){
        if(serializedFile.exists()){
            serializedFile.delete();
        }
        this.lst = new WriteOnceReadManyArrayList<String>(folder,name);
    }
    
    @After
    public void tearDown(){
        if(serializedFile.exists()){
            serializedFile.delete();
        }
        this.lst.close();
    }
    
    @Test
    public void test() {
        for(int i=0;i<5;i++){
            this.lst.add(Integer.toString(i));
        }
        for(int i=0;i<5;i++){
            Assert.assertEquals(Integer.toString(i), this.lst.get(i));
        }
        
        Assert.assertEquals(5, this.lst.size());
    }

}
