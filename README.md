Large Collections
================

HashMap implementation that uses Off-Heap Memory to Scale to millions of elements without running in to GC errors. 
It utilizes the LevelDB project from Google to achieve this. It implements the default java.util.Map class so users can 
simply use the API they are familiar with.

It sticks to the spirit of Convention over Customization. By default it uses the folder defined by the System Property 
java.io.tmpdir to store the files it needs to support the datastore which backs the map. However the user can customize the
folder and the name of the datastore using various constructor. It support serialization and deserialization. 

Currently supports 3 main classes-
1. OffHeapMap - This is a fastest implementation. It does not attempt to maintain information of the size of the Map. The
getSize() function will return a hueristic size. Use it if you want a Fast implementation but do not care about the size

2. CacheMap - This is an extension of OffHeapMap but it does housekeeping to ensure that the size is accurately tracked at
all times. Put's are about 30% slower since a Put needs to be preceded by a Get to determine if the size counter needs to be
incremented. Likewise of remove() calls

3. LargeCacheMap - This is a slight departure from the java.util.Map interface. By java.util.Map supports size() as a 
integer return value which has a maximum value of the range of 2 billion. Often this is adequate. However if you need more,
use LargeCacheMap. There is an additional function provided called lsize() which returns a long value. The size() still works
but it will not be accurate once your size crosses the Integer.MAX_VALUE count.

<b>Usage</b>-

By default just use
java.util.Map<MyKeyClass,MyValueClass> m = new OffHeapMap<MyKeyClass,MyValueClass>();<br/>
//Use like a regular java.util.Map<br/>
At the end clean up as follows<br/>
((java.io.Closeable)m).close()<br/>
