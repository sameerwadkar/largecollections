package utils;

import static org.junit.Assert.*;
import junit.framework.Assert;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Test;

public class SerializationUtilsTest {

    @Test
    public void testSerializable() {
        SerializationUtils<Integer,Long> su = new SerializationUtils<Integer,Long>();
        byte[] k = su.serializeKey(new Integer(1));
        Integer i = su.deserializeKey(k);
        Assert.assertEquals(1, i.intValue());
        
        byte[] v = su.serializeValue(new Long(1));
        Long l = su.deserializeValue(v);
        Assert.assertEquals(1l, l.longValue());
    }

    
    @Test
    public void testKeyWritableValueSerializable() {
        SerializationUtils<IntWritable,Long> sw = new SerializationUtils<IntWritable,Long>();
        byte[] kw = sw.serializeKey(new IntWritable(1));
        IntWritable i = sw.deserializeKey(kw);
        Assert.assertEquals(1, i.get());
    }
    
    @Test
    public void testKeyWritableValueWritable() {
        SerializationUtils<IntWritable,LongWritable> sw = new SerializationUtils<IntWritable,LongWritable>();
        byte[] kw = sw.serializeValue(new LongWritable(1));
        LongWritable i = sw.deserializeValue(kw);
        Assert.assertEquals(1l, i.get());
    }
}
