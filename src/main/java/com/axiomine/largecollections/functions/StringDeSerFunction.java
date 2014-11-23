package com.axiomine.largecollections.functions;

import com.google.common.base.Function;


public class StringDeSerFunction implements Function<byte[],String>{
    public String apply(byte[] arg) {
        return new String(arg);
    }    
}
