package com.axiomine.largecollections.functions;

import com.google.common.base.Function;
import com.google.common.primitives.Longs;

public class LongSerFunction implements Function<Long,byte[]>{
    public byte[] apply(Long arg) {
        return Longs.toByteArray(arg);
    }    
}
