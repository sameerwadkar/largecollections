package com.axiomine.largecollections.functions;

import com.google.common.base.Function;

public class StringSerFunction implements Function<String,byte[]>{
    public byte[] apply(String arg) {
        return arg.getBytes();
    }    
}
