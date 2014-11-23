package com.axiomine.largecollections.functions;


import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import org.apache.hadoop.io.Writable;

import com.google.common.base.Function;
import com.google.common.base.Throwables;


public class WritableSerFunction implements Function<Writable,byte[]>{
    public byte[] apply(Writable arg) {
        return serializeWritable(arg);
    }    
    private byte[] serializeWritable(Writable writable) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream dataOut = new DataOutputStream(out);
            writable.write(dataOut);
            dataOut.close();
            return out.toByteArray();
        } catch (Exception ex) {
            Throwables.propagate(ex);
        }
        return null;
    }
}
