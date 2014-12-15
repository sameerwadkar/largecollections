Large Collections (RETIRED)
================
This library has not been replaced by https://github.com/sameeraxiomine/LargeCollections which provides significantly improved performance.

Java Collections implementation that uses Off-Heap Memory to Scale to millions of elements without running in to GC errors. 
It utilizes the LevelDB project from Google to achieve this. 

Currrently this API supports three types of Collections
1. java.util.Map
2. java.util.List (WriteOnce Read Many times semantics)
3. java.util.Set

All key/value classes need to support either java.io.Serializable or org.apache.hadoop.io.Writable.

<b>Caution</b>: Do not mix Serializable with Writable. For example, do not use a List<Writable>. It will not work. Use a ArrayWritable or some extension of ArrayWritable in situations where you need to use List<Writable>

It sticks to the spirit of "Convention over Customization". By default it uses the folder defined by the System Property 
"java.io.tmpdir" to store the files it needs to support the datastore which backs the map. However the user can customize the folder and the name of the datastore using various constructors. It support serialization and deserialization as well. 

Currently supports 3 main classes-
1. FastCacheMap - This is a fastest implementation. It does not attempt to maintain information of the size of the Map. The
getSize() function will return a hueristic size. Use it if you want a Fast implementation but do not care about the size

2. CacheMap - This is an extension of OffHeapMap but it does housekeeping to ensure that the size is accurately tracked at
all times. Put's are about 30% slower since a Put needs to be preceded by a Get to determine if the size counter needs to be
incremented. Likewise for remove() calls

3. CacheSet - This is an implementation of the java.util.Set. Maximum number of elements it can hold is Integer.MAX_VALUE

There is also two special factory classes called FactoryMap or FactoryList. The former has a method getMap(String mapName) and the latter supports a getList(String listName). The idea is to use a single LevelDB backing database for a family of Maps or Lists. The other classes use their own LevelDB backing database.

The library utilizes the Google Guava Libraries. It uses BloomFilters from the Guava library to achieve high performance. When using FactoryMap if you make a large number of deletes make sure you occassionally call optimize() on the MapFactory to recreate the bloom filters. This is necessary because, it is not possible to delete records from a bloom filter. This leads to the mightContain() returning true for a records deleted from a Map. 



Usage
================

By default just use

```java
java.util.Map<MyKeyClass,MyValueClass> m = new CacheMap<MyKeyClass,MyValueClass>();
//Or java.util.Map<MyKeyClass,MyValueClass> m = new FastCacheMap<MyKeyClass,MyValueClass>();
//Or java.util.Set<MyClass> m = new CacheSet<MyClass>();
//Use like a regular java.util.Map
/*At the end clean up as follows*/
((java.io.Closeable)m).close();
```
Remember all activity with the collections is by-value semantics unlike a typical Java collections which follows by-reference semantics
with respect to their contents. The reason being, LargeCollections uses LevelDB as its underlying storage. All key/value instances are stored
as serialized byte[] arrays. Retrieving them requires deserialization from a byte[] array. Any changes to these instances will not be reflected 
in the underlying LevelDB storage. Hence if you make changes to these instances after retrieving them from the underlying collection, store it back 
once you have finished making the changes to ensure that the next retrieval sees the changes made.


