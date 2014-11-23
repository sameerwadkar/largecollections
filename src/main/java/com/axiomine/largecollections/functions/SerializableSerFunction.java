package com.axiomine.largecollections.functions;

import java.io.Serializable;

import com.google.common.base.Function;


public class SerializableSerFunction implements Function<Serializable,byte[]>{
    public byte[] apply(Serializable arg) {
        return org.apache.commons.lang.SerializationUtils.serialize((Serializable)arg);
    }    
}
