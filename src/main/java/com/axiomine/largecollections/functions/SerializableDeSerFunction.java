package com.axiomine.largecollections.functions;

import java.io.Serializable;

import com.google.common.base.Function;


public class SerializableDeSerFunction implements Function<byte[],Serializable>{
    public Serializable apply(byte[] arg) {
        return (Serializable) org.apache.commons.lang.SerializationUtils.deserialize(arg);
    }    
}
