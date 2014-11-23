package com.axiomine.largecollections.functions;

import com.google.common.base.Function;
import com.google.common.primitives.Ints;

public class IntegerDeSerFunction implements Function<byte[],Integer>{
    public Integer apply(byte[] arg) {
        return Ints.fromByteArray(arg);
    }    
}
