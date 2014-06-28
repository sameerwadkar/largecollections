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

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

import utils.SerializationUtils;
/**
 * WriteOnceReadManyArrayList is an implementation of java.util.List. It allows you to write
 * once and read many times. An element can be added to the list but cannot be removed.
 * 
 */
public class WriteOnceReadManyArrayList<V> implements List<V>,Serializable,Closeable {
    public  static final long serialVersionUID = 1l;
    protected CacheMap<Integer,V> valueByIndex = null;
    protected CacheMap<V,Integer> indexByValue = null;
    protected transient SerializationUtils<Object,V> sdUtils = new SerializationUtils<Object,V>();
    
    public WriteOnceReadManyArrayList(){
        valueByIndex = new CacheMap<Integer,V>();
        indexByValue = new CacheMap<V,Integer>();
        
    }
    public int size() {
        // TODO Auto-generated method stub
        return valueByIndex.size();
    }

    public boolean isEmpty() {
        // TODO Auto-generated method stub
        return valueByIndex.size()==0;
    }

    public boolean contains(Object o) {
        // TODO Auto-generated method stub
        return (indexByValue.get(o)!=null);
    }

    public Iterator<V> iterator() {
        // TODO Auto-generated method stub
        return new MyIterator<V>(this);
    }

    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    public <T> T[] toArray(T[] a) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    public boolean add(V e) {
        // TODO Auto-generated method stub
        if(this.indexByValue.containsKey(e)){
            return false;
        }
        else{
            int index = this.indexByValue.size();
            this.indexByValue.put(e, index);
            this.valueByIndex.put(index, e);
            return true;
        }

    }
    
   

    public boolean remove(Object o) {
        throw new UnsupportedOperationException();    
    }

    public boolean containsAll(Collection<?> c) {
        // TODO Auto-generated method stub
        boolean containsAll = true;
        for(Object v:c){
            if(!this.indexByValue.containsKey(v)){
                containsAll=false;
                break;
            }
        }
        return containsAll;
    }

    public boolean addAll(Collection<? extends V> c) {
        for(Object v:c){
            this.add((V)v);
        }
        return true;
    }

    public boolean addAll(int index, Collection<? extends V> c) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();    
    }

    public boolean removeAll(Collection<?> c) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();    
    }

    public boolean retainAll(Collection<?> c) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();    
    }

    public void clear() {
        // TODO Auto-generated method stub
        this.indexByValue.clear();
        this.valueByIndex.clear();
        
    }

    public V get(int index) {
        // TODO Auto-generated method stub
        return this.valueByIndex.get(index);
    }

    public V set(int index, V element) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();    
    }

    public void add(int index, V element) {
        throw new UnsupportedOperationException();    
        
    }

    public V remove(int index) {
        throw new UnsupportedOperationException();    
    }

    public int indexOf(Object o) {
        // TODO Auto-generated method stub
       return this.indexByValue.get(0);
    }

    public int lastIndexOf(Object o) {
        // TODO Auto-generated method stub
        return this.indexByValue.get(0);
    }

    public ListIterator<V> listIterator() {
        return new MyListIterator(this);
    }

    public ListIterator<V> listIterator(int index) {
        // TODO Auto-generated method stub
        return new MyListIterator(this);
    }

    public List<V> subList(int fromIndex, int toIndex) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();    
    }
    
    private void writeObject(java.io.ObjectOutputStream stream)
            throws IOException {
        stream.writeObject(this.indexByValue);
        stream.writeObject(this.valueByIndex);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException,
            ClassNotFoundException {
        this.indexByValue = (CacheMap<V,Integer>) in.readObject();
        this.valueByIndex = (CacheMap<Integer,V>) in.readObject();

        
    }

    private final class MyIterator<V> implements Iterator<V> {
        private WriteOnceReadManyArrayList list = null;

        int size = -1;
        int index =-1;
        protected MyIterator(WriteOnceReadManyArrayList list){
           this.list = list;
           this.size=list.size();
        }
        public boolean hasNext() {
            return (this.index<(this.size-1));
        }

        public V next() {
            V v = null;
            if(this.index<(this.size-1)){
                this.index++;
                v = (V)this.list.valueByIndex.get(this.index); 
                
            }
            else{
                throw new NoSuchElementException();
            }
            return v;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
        
    }
    private final class MyListIterator<V> implements ListIterator<V> {
        private WriteOnceReadManyArrayList list = null;

        int size = -1;
        int index =-1;
        protected MyListIterator(WriteOnceReadManyArrayList list){
           this.list = list;
           this.size=list.size();
        }
        protected MyListIterator(WriteOnceReadManyArrayList list,int index){
            list = list;
            this.size=list.size();
            this.index = this.index-1;
            if(this.index<0 || this.index>(this.size+1)){
                throw new IndexOutOfBoundsException();
            }
         }
        public boolean hasNext() {
            return (this.index<(this.size-1));
        }

        public V next() {
            // TODO Auto-generated method stub
            V v = null;
            if(this.index<(this.size-1)){
                this.index++;
                v = (V)this.list.valueByIndex.get(this.index); 
                
            }
            else{
                throw new NoSuchElementException();
            }
            return v;
        }

        public boolean hasPrevious() {
            // TODO Auto-generated method stub
            return this.index>0;  
        }

        public V previous() {
            // TODO Auto-generated method stub
            V v = null;
            if(index>0){
                this.index--;
                v = (V)this.list.valueByIndex.get(this.index);                
            }
            else{
                throw new NoSuchElementException();
            }
            return v;
        }

        public int nextIndex() {
            return (this.index+1);
        }

        public int previousIndex() {
            return  (this.index-1);
        }

        public void remove() {
            throw new UnsupportedOperationException();    
            
        }

        public void set(V e) {
            this.list.set(this.index, e);
            
        }

        public void add(V e) {
            throw new UnsupportedOperationException();
        }    
    }
    public void close() throws IOException {
        this.indexByValue.close();
        this.valueByIndex.close();        
    }
}
