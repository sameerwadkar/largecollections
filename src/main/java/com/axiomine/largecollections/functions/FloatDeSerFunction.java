package com.axiomine.largecollections.functions;

import java.nio.ByteBuffer;

import com.google.common.base.Function;


public class FloatDeSerFunction implements Function<byte[],Float>{
    public Float apply(byte[] arg) {
        return ByteBuffer.wrap(arg).getFloat();
    }    
}
