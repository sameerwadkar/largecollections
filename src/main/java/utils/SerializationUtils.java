/*
 * Copyright 2014 Sameer Wadkar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;

import com.google.common.base.Throwables;

public class SerializationUtils<K,V> implements Serializable{
    private boolean keyTypeVerified = false;
    private boolean valueTypeVerified = false;

    private boolean isKeyWritable;
    private boolean isValueWritable;
    
    private boolean isKeyString;
    private boolean isValueString;
    
    private K key;
    private V value;
    
    public static char sep = '\0';
    public static String sepStr = Character.toString(sep);
    
    /*
    public Class returnedKeyClass() {
        ParameterizedType parameterizedType = (ParameterizedType)this.getClass()
                                                    .getGenericSuperclass();
        System.out.println("TTT"+parameterizedType);
        System.out.println("TTT"+parameterizedType.getActualTypeArguments()[0]);
       
        
        return (Class) parameterizedType.getActualTypeArguments()[0];
   }
    public Class returnedValueClass() {
        ParameterizedType parameterizedType = (ParameterizedType)getClass()
                                                    .getGenericSuperclass();
        return (Class) parameterizedType.getActualTypeArguments()[1];
   }
   */
    public SerializationUtils(){
    
    }
   

    public void determineKeyType(K k) {
        
        if (!keyTypeVerified) {
            keyTypeVerified = true;
            try {
                if (k instanceof Writable) {
                    isKeyWritable = true;
                    key = (K) k.getClass().newInstance();
                }
                else if(k instanceof java.lang.String){
                    isKeyString = true;
                    key = (K) k.getClass().newInstance();
                }
            } catch (Exception ex) {
                throw Throwables.propagate(ex);
            }
        }

    }

    public void determineValueType(V v) {
        if (!valueTypeVerified) {
            valueTypeVerified = true;
            try {
                if (v instanceof Writable) {
                    isValueWritable = true;
                    value = (V) v.getClass().newInstance();
                }
                else if(v instanceof java.lang.String){
                    isValueString = true;
                    value = (V) v.getClass().newInstance();
                }
            } catch (Exception ex) {
                throw Throwables.propagate(ex);
            }

        }

    }

    public byte[] serializeKey(K key) {
        determineKeyType(key);
        if (this.isKeyWritable) {
            return this.serializeWritable((Writable) key);
        } 
        else if(this.isKeyString){
            return ((String)key).getBytes();
        }
        else {
            return org.apache.commons.lang.SerializationUtils.serialize((java.io.Serializable)key);
        }

    }

    public byte[] serializeValue(V value) {
        determineValueType(value);
        if (this.isValueWritable) {
            return this.serializeWritable((Writable) value);
        } 
        else if(this.isValueString){
            return ((String)value).getBytes();
        }
        else {
            return org.apache.commons.lang.SerializationUtils.serialize((java.io.Serializable)value);
        }
    }

    public K deserializeKey(byte[] bytes) {
        
        if (this.isKeyWritable) {
            return (K) this.deserializeWritableKey(bytes);
        } 
        else if(this.isKeyString){
            return (K) new String(bytes);
        }
        else {
            return (K)org.apache.commons.lang.SerializationUtils.deserialize(bytes);
        }

    }

    public V deserializeValue(byte[] bytes) {
        determineValueType(value);
        if (this.isValueWritable) {
            return (V) this.deserializeWritableValue(bytes);
        } 
        else if(this.isValueString){
            return (V) new String(bytes);
        }
        else {
            return (V)org.apache.commons.lang.SerializationUtils.deserialize(bytes);
        }
    }



    private byte[] serializeWritable(Writable writable) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream dataOut = new DataOutputStream(out);
            writable.write(dataOut);
            dataOut.close();
            return out.toByteArray();
        } catch (Exception ex) {
            Throwables.propagate(ex);
        }
        return null;
    }



    private Object deserializeWritableKey(byte[] bytes) {
        try {
            Writable writable = (Writable) key.getClass().newInstance();
            return deserializeWritable(writable, bytes);
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }

    }

    private Object deserializeWritableValue(byte[] bytes) {
        try {
            Writable writable = (Writable) value.getClass().newInstance();
            return deserializeWritable(writable, bytes);
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }

    }

    private static Writable deserializeWritable(Writable writable, byte[] bytes) {
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(bytes);
            DataInputStream dataIn = new DataInputStream(in);
            writable.readFields(dataIn);
            dataIn.close();
            return writable;
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }

    }
    
    
    public   byte[] serializeKey(String cacheName,K key) {
        determineKeyType(key);
        byte[] cNameBArry = null;
        byte[] bArray = null;
        try{
            cNameBArry = (cacheName+sepStr).getBytes();
            bArray = this.serializeKey(key);
            
            byte[] combined = new byte[cNameBArry.length + bArray.length];
            System.arraycopy(cNameBArry,0,combined,0         ,cNameBArry.length);
            System.arraycopy(bArray,0,combined,cNameBArry.length,bArray.length);
            return combined;
        }
        catch(Exception ex){
            throw new RuntimeException(ex);
        }
    }
    
    
    private void writeObject(java.io.ObjectOutputStream stream)
            throws IOException {
        stream.writeBoolean(keyTypeVerified);
        stream.writeBoolean(valueTypeVerified);
        stream.writeBoolean(isKeyWritable);
        stream.writeBoolean(isValueWritable);
        stream.writeBoolean(isKeyString);
        stream.writeBoolean(isValueString);
        stream.writeObject(key);
        stream.writeObject(value);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException,
            ClassNotFoundException {
        keyTypeVerified = in.readBoolean();
        valueTypeVerified = in.readBoolean();
        isKeyWritable = in.readBoolean();
        isValueWritable = in.readBoolean();
        isKeyString = in.readBoolean();
        isValueString = in.readBoolean();
        key = (K)in.readObject();
        value = (V) in.readObject();
 
    }
    public static void main(String[] args){
        SerializationUtils<String,String> x = new SerializationUtils<String,String>();
        x.determineKeyType("");
        
        //System.out.println(x.returnedValueClass());
    }
    
    

}
