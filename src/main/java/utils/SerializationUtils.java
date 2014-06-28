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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.ParameterizedType;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Throwables;

public class SerializationUtils<K, V> {
    private boolean keyTypeVerified = false;
    private boolean valueTypeVerified = false;

    private boolean isKeyWritable;
    private boolean isValueWritable;
    private K key;
    private V value;

   

    private void determineKeyType(K k) {
        if (!keyTypeVerified) {
            keyTypeVerified = true;
            try {
                if (k instanceof Writable) {
                    isKeyWritable = true;
                    key = (K) k.getClass().newInstance();
                }
            } catch (Exception ex) {
                throw Throwables.propagate(ex);
            }
        }

    }

    private void determineValueType(V v) {
        if (!valueTypeVerified) {
            valueTypeVerified = true;
            try {
                if (v instanceof Writable) {
                    isValueWritable = true;
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
        } else {
            return org.apache.commons.lang.SerializationUtils.serialize((java.io.Serializable)key);
        }

    }

    public byte[] serializeValue(V value) {
        determineValueType(value);
        if (this.isValueWritable) {
            return this.serializeWritable((Writable) value);
        } else {
            return org.apache.commons.lang.SerializationUtils.serialize((java.io.Serializable)value);
        }
    }

    public K deserializeKey(byte[] bytes) {
        determineKeyType(key);
        if (this.isKeyWritable) {
            return (K) this.deserializeWritableKey(bytes);
        } else {
            return (K)org.apache.commons.lang.SerializationUtils.deserialize(bytes);
        }

    }

    public V deserializeValue(byte[] bytes) {
        determineValueType(value);
        if (this.isValueWritable) {
            return (V) this.deserializeWritableValue(bytes);
        } else {
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

}
