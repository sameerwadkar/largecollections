package com.axiomine.largecollections.functions;

import java.nio.ByteBuffer;

import com.google.common.base.Function;


public class DoubleSerFunction implements Function<Double,byte[]>{
    public byte[] apply(Double arg) {
        byte [] bytes = ByteBuffer.allocate(8).putDouble(arg).array();
        return bytes;
    }    
}
