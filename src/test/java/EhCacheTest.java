///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//import org.ehcache.Cache;
//import org.ehcache.CacheManager;
//import org.ehcache.config.builders.CacheConfigurationBuilder;
//import org.ehcache.config.builders.CacheEventListenerConfigurationBuilder;
//import org.ehcache.config.builders.CacheManagerBuilder;
//import org.ehcache.config.builders.ResourcePoolsBuilder;
//import org.ehcache.config.units.MemoryUnit;
//import org.ehcache.event.CacheEventListener;
//import org.ehcache.event.EventType;
//import org.ehcache.config.units.EntryUnit;
//import org.ehcache.spi.serialization.Serializer;
//import org.ehcache.spi.serialization.SerializerException;
//import sun.misc.VM;
//
//import java.io.File;
//import java.lang.ref.SoftReference;
//import java.nio.ByteBuffer;
//import java.util.ArrayList;
//import java.util.Random;
//
//public class EhCacheTest {
//
//  static int storedInSecondaryCacheCount = 0;
//  // TODO fix initialization order: create secondary cache first
//  static Cache<Long, Node> cache = null;
//  static Cache<Long, Node> secondaryCache = null;
//
//  public static void main(String[] args) throws Exception {
//    File ondiskCacheFile = new File("ehcache-" + System.currentTimeMillis());
//    Random random = new Random();
//
//    CacheEventListener<Long, Node> cacheEventListener = event -> {
//      if (event.getType().equals(EventType.REMOVED)) {
//        secondaryCache.remove(event.getKey());
//      } else if (event.getType().equals(EventType.EVICTED)) {
//        storedInSecondaryCacheCount++;
//        // evicted from on-heap cache -> serialize and store in off-heap datastore (which overflows to disk)
//        final Node node = event.getOldValue();
//        try {
//          secondaryCache.put(event.getKey(), node);
//        } catch (Throwable t) {
//          t.printStackTrace();
//        }
//      }
//    };
//    CacheEventListenerConfigurationBuilder cacheEventListenerConfig = CacheEventListenerConfigurationBuilder
//        .newEventListenerConfiguration(cacheEventListener, EventType.EVICTED, EventType.REMOVED).asynchronous().unordered();
//
//    long offheapBytes = VM.maxDirectMemory() - 1024*1024;
//    // using two caches because when using a tiered cache, ehcache serializes every entry, even if it's still in the heap tier
//    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
//        .withCache("onHeap",
//            CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, Node.class,
//                ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10000, EntryUnit.ENTRIES)) //large enough to not suffer from re-serialization cost when creating a node and setting properties straight afterwards
//                .add(cacheEventListenerConfig))
//        .withCache("offHeapWithDiskOverflow",
//            CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, Node.class,
//            ResourcePoolsBuilder.newResourcePoolsBuilder()
//                .heap(10000, EntryUnit.ENTRIES) // implementation detail of ehcache, would rather not have this one
//                .offheap(offheapBytes, MemoryUnit.B)
//                .disk(Long.MAX_VALUE, MemoryUnit.B, true)) //unbounded
//        .withValueSerializer(new NodeSerializer()))
//        .with(CacheManagerBuilder.persistence(ondiskCacheFile))
//        .build();
//    cacheManager.init();
//
//    secondaryCache = cacheManager.getCache("offHeapWithDiskOverflow", Long.class, Node.class);
//    cache = cacheManager.getCache("onHeap", Long.class, Node.class);
//
//    int numberOfNodes = 1000000;
//    ArrayList<NodeRef> nodeRefs = new ArrayList<>(numberOfNodes);
//    for (long id = 0; id < numberOfNodes; id++) {
//      byte[] data = new byte[1024 * 2];
//      random.nextBytes(data);
//      Node node = new Node(id, data);
//      nodeRefs.add(new NodeRef(node));
//      cache.put(id, node);
//
//      long count = id + 1;
//      if (count % 10000 == 0) {
//        // allow diskwriter to catch up and avoid OOM error -> TODO implement (or configure?) some backpressure when serializing to avoid OOM?
//        Thread.sleep(100);
//      }
//      if (count % 100000 == 0) {
//        System.out.println("created " + count + " nodes");
//      }
//    }
//
//    Thread.sleep(1000);
//    System.out.println("storedInSecondaryCacheCount=" + storedInSecondaryCacheCount);
//    System.out.println("serializedCount=" + NodeSerializer.serializedCount);
//
//    // verify we can get all back from cache
////    for (int i : new int[] {5000, numberOfNodes - 100}) {
////    for (int i = 0; i < numberOfNodes; i++) {
////      System.out.println(i + ": in cache: " + cache.containsKey(i) + "; in 2nd: " + secondaryCache.containsKey(i));
////      if (!cache.containsKey(i) && !secondaryCache.containsKey(i)) {
////        throw new AssertionError("key in neither of two caches: " + i);
////      }
////      nodeRefs.get(i).get();
////    }
//
//    cacheManager.close();
//  }
//}
//
//class Node {
//  private final long id;
//  private final byte[] data;
//
//  Node(long id, byte[] data) {
//    this.id = id;
//    this.data = data;
//  }
//
//  public byte[] getData() {
//    return data;
//  }
//
//  public long getId() {
//    return id;
//  }
//}
//
//class NodeSerializer implements Serializer<Node> {
//  static long serializedCount = 0;
//  @Override
//  public ByteBuffer serialize(Node node) throws SerializerException {
//    serializedCount++;
////    if (serializedCount % 10000 == 0) {
////      System.out.println("serialized " + serializedCount);
////    }
//    return ByteBuffer.wrap(node.getData());
//  }
//
//  @Override
//  public Node read(ByteBuffer binary) throws ClassNotFoundException, SerializerException {
//    return new Node(-1l, binary.array()); //TODO read id properly (use msgpack)
//  }
//
//  @Override
//  public boolean equals(Node object, ByteBuffer binary) throws ClassNotFoundException, SerializerException {
//    throw new RuntimeException("not implemented");
//  }
//}
//
//class NodeRef {
//  public final long id;
//  protected SoftReference<Node> softReference;
//
//  NodeRef(Node node) {
//    this.id = node.getId();
//    this.softReference = new SoftReference<>(node);
//  }
//
//  public Node get() {
//    final Node fromSoftRef = softReference.get();
//    if (fromSoftRef != null) {
//      return fromSoftRef;
//    } else {
//      try {
//        final Node node;
//        if (EhCacheTest.cache.containsKey(id)) {
//          node = EhCacheTest.cache.get(id);
//        } else if (EhCacheTest.secondaryCache.containsKey(id)) {
//          node = EhCacheTest.secondaryCache.get(id);
//        } else {
//          throw new AssertionError("id not found in either of two caches: " + id);
//        }
//        this.softReference = new SoftReference<>(node);
//        return node;
//      } catch (Exception e) {
//        throw new RuntimeException(e);
//      }
//    }
//  }
//}
