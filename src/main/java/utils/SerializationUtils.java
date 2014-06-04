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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class SerializationUtils<K> {

    private boolean isByteArray=false;
    
    
    public  byte[] serialize(K obj) {

        try{
            ByteArrayOutputStream b = new ByteArrayOutputStream();
            ObjectOutputStream o = new ObjectOutputStream(b);
            o.writeObject(obj);
            return b.toByteArray();
        }
        catch(Exception ex){
            throw new RuntimeException(ex);
        }
    }
    
    public   Object deserialize(byte[] bytes)  {
        if(bytes instanceof byte[]){
            return bytes;
        }
        try{
        ByteArrayInputStream b = new ByteArrayInputStream(bytes);
        ObjectInputStream o = new ObjectInputStream(b);
        return o.readObject();
        }
        catch(Exception ex){
            throw new RuntimeException(ex);
        }
    }
    
    
}
