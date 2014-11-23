package com.axiomine.largecollections.functions;

import com.google.common.base.Function;
import com.google.common.primitives.Ints;

public class IntegerSerFunction implements Function<Integer,byte[]>{
    public byte[] apply(Integer arg) {
        return Ints.toByteArray(arg);
    }    
}
