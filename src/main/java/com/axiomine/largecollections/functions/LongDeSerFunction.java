package com.axiomine.largecollections.functions;

import com.google.common.base.Function;
import com.google.common.primitives.Longs;

public class LongDeSerFunction implements Function<byte[],Long>{
    public Long apply(byte[] arg) {
        return Longs.fromByteArray(arg);
    }    
}
