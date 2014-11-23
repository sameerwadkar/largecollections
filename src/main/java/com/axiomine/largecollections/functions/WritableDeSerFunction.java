package com.axiomine.largecollections.functions;


import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

import org.apache.hadoop.io.Writable;

import com.google.common.base.Function;
import com.google.common.base.Throwables;


public class WritableDeSerFunction implements Function<byte[],Writable>{
    private Class<Writable> writableCls = null; 
    public WritableDeSerFunction(Class<Writable> writableCls){
        this.writableCls = writableCls;
    }
    public Writable apply(byte[] arg) {
        return deserializeWritableValue(arg);
    }    

    private Writable deserializeWritableValue(byte[] bytes) {
        try {
            Writable writable =  this.writableCls.newInstance();
            return deserializeWritable(writable, bytes);
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }

    }
    private static Writable deserializeWritable(Writable writable, byte[] bytes) {
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(bytes);
            DataInputStream dataIn = new DataInputStream(in);
            writable.readFields(dataIn);
            dataIn.close();
            return writable;
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }

    }
}
