package com.axiomine.largecollections.functions;

import java.nio.ByteBuffer;

import com.google.common.base.Function;


public class FloatSerFunction implements Function<Float,byte[]>{
    public byte[] apply(Float arg) {
        byte [] bytes = ByteBuffer.allocate(4).putFloat(arg).array();
        return bytes;
    }    
}
