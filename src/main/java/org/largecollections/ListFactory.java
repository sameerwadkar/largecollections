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
package org.largecollections;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.Closeable;
import java.util.List;

import com.google.common.base.Throwables;

/**
 * WriteOnceReadManyArrayList is an implementation of java.util.List. It allows
 * you to write once and read many times. An element can be added to the list
 * but cannot be removed.
 * 
 */
public class ListFactory<V> implements Closeable {
  
    protected MapFactory factory = null;
    public ListFactory(String factoryFolder, String factoryName, int cacheSize){
        this.factory = new MapFactory(factoryFolder, factoryName, cacheSize);
    }
    protected  List<V> getInstance(String lstName) {        
         return new WriteOnceReadManyArrayList<V>(this.factory,lstName);
    }
    public void close() {
        try {
            factory.close();

        } catch (Exception ex) {
            Throwables.propagate(ex);
        }
    }
}
