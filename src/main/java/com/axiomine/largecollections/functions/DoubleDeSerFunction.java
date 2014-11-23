package com.axiomine.largecollections.functions;

import java.nio.ByteBuffer;

import com.google.common.base.Function;


public class DoubleDeSerFunction implements Function<byte[],Double>{
    public Double apply(byte[] arg) {
        return ByteBuffer.wrap(arg).getDouble();
    }    
}
