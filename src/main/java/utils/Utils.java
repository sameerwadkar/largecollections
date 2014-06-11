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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.largecollections.OffHeapMap;

import com.google.common.base.Throwables;

public class Utils {
    public  static void serialize(Object obj,File f) {
        try{
            FileOutputStream fileOut =
                    new FileOutputStream(f);
            ByteArrayOutputStream b = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(obj);
            out.close();
            fileOut.close();
        }
        catch(Exception ex){
            throw new RuntimeException(ex);
        }
    }

    public  static Object deserialize(File f) {
        try
        {
           FileInputStream fileIn = new FileInputStream(f);
           ObjectInputStream in = new ObjectInputStream(fileIn);
           Object m= (Object) in.readObject();
           in.close();
           fileIn.close();
           return m;
        }catch(Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

    public  static byte[] serialize(Object obj) {
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
    
    public  static byte[] serialize(String cacheName,Object obj) {
        try{
            byte[] nameBArry = (cacheName+'\0').getBytes();
            ByteArrayOutputStream b = new ByteArrayOutputStream();
            ObjectOutputStream o = new ObjectOutputStream(b);
            o.writeObject(obj);
            byte[] bArray = b.toByteArray();
            byte[] combined = new byte[nameBArry.length + bArray.length];

            System.arraycopy(nameBArry,0,combined,0         ,nameBArry.length);
            System.arraycopy(bArray,0,combined,nameBArry.length,bArray.length);
            return b.toByteArray();
        }
        catch(Exception ex){
            throw new RuntimeException(ex);
        }
    }

    
    public static  Object deserialize(byte[] bytes)  {
        try{
        ByteArrayInputStream b = new ByteArrayInputStream(bytes);
        ObjectInputStream o = new ObjectInputStream(b);
        return o.readObject();
        }
        catch(Exception ex){
            throw new RuntimeException(ex);
        }
    }
    
    public  static byte[] serialize(byte[] obj) {
       return obj;
    }
    


    public static int sizeof(Class dataType)
    {
        if (dataType == null) throw new NullPointerException();

        if (dataType == int.class    || dataType == Integer.class)   return 4;
        if (dataType == short.class  || dataType == Short.class)     return 2;
        if (dataType == byte.class   || dataType == Byte.class)      return 1;
        if (dataType == char.class   || dataType == Character.class) return 2;
        if (dataType == long.class   || dataType == Long.class)      return 8;
        if (dataType == float.class  || dataType == Float.class)     return 4;
        if (dataType == double.class || dataType == Double.class)    return 8;

        return 4; // 32-bit memory pointer... 
                  // (I'm not sure how this works on a 64-bit OS)
    }
    

    public  static void delete(File f) throws IOException {
        if (f.isDirectory()) {
            System.err.println("DD");
          for (File c : f.listFiles()){
              System.err.println("DD="+c.getAbsolutePath());
              FileUtils.deleteQuietly(c);
              System.err.println("Deleted="+c.getAbsolutePath());
          }
        }
      }
    public  static void deleteOffHeapMap(Map m)  {
        try{
            OffHeapMap ohm = (OffHeapMap)m;
            ohm.close();     
        }
        catch(Exception ex){
            Throwables.propagate(ex);
        }

      }
}
