/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package manual;

import com.sun.management.GarbageCollectionNotificationInfo;

import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.MemoryUsage;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;

public class ManuallyManagedRefs {

  // TODO store elsewhere
  public static int instancesAwaitingSerialization = 0;

  /** run with -XX:+UseG1GC -Xms256m -Xmx256m */
  public static void main(String[] args) throws Exception {
    int numberOfNodes = 2000000;
    int backpressureAppliedCount = 0;

    // TODO find the best list/map impl: concurrently adding/iterating/removing elements
    ConcurrentLinkedDeque<NodeRef> nodeRefs = new ConcurrentLinkedDeque<>();
    new ReferenceManager(nodeRefs);
    for (long id = 0; id < numberOfNodes; id++) {
      if (instancesAwaitingSerialization > 0) {
        // TODO measure how often backpressure was applied
        Thread.sleep(100); //apply some backpressure - this must be longer than the average time to serialize *one* instance
        backpressureAppliedCount++;
      }
      byte[] data = new byte[1024 * 2];
      Node node = new Node(id, data);
      nodeRefs.add(new NodeRef(node));

      long count = id + 1;
      if (count % 10000 == 0) {
        System.out.println("lastSerializedTime " + count + " nodes; backpressureAppliedCount=" + backpressureAppliedCount);
        Thread.sleep(100);
      }
    }
  }

}

class Node {
  private final long id;
  private final byte[] data;

  Node(long id, byte[] data) {
    this.id = id;
    this.data = data;
  }

  public byte[] getData() {
    return data;
  }

  public long getId() {
    return id;
  }
}

class NodeRef {
  public final long id;
  private Node node;

  public static int clearedInstances = 0;
  public static long nanosSpentSerializing = 0;


  NodeRef(Node node) {
    this.id = node.getId();
    this.node = node;
  }

  public Node get() {
    // TODO: if null, load from cache
    return node;
  }

  //TODO make package-protected to ensure only RefMgr can call it
  public void clear() {
    clearedInstances++;
    ManuallyManagedRefs.instancesAwaitingSerialization++;

    // emulate long running action in lieu of serializer
    nanosSpentSerializing += bubbleSort(500); //Thread.sleep seems to have other overheads
    node = null;
    if (clearedInstances % 10000 == 0) {
      System.out.println("cleared " + clearedInstances + " in total; average time spent serializing: " + ((float) nanosSpentSerializing / (float) clearedInstances / 1000f / 1000f) + "ms");
    }
    ManuallyManagedRefs.instancesAwaitingSerialization--;
  }

  public boolean isSet() {
    return node != null;
  }

  public boolean isCleared() {
    return node == null;
  }

  /* returns the time taken (nanos) */
  public static long bubbleSort(int arrLength) {
    long start = System.nanoTime();
    int[] arr = new Random().ints(arrLength).toArray();
    int temp = 0;
    for(int i=0; i < arrLength; i++){
      for(int j=1; j < (arrLength-i); j++){
        if(arr[j-1] > arr[j]){
          //swap elements
          temp = arr[j-1];
          arr[j-1] = arr[j];
          arr[j] = temp;
        }
      }
    }
    return System.nanoTime() - start;
  }
}

class ReferenceManager {
  protected final ConcurrentLinkedDeque<NodeRef> nodeRefs;

  public ReferenceManager(ConcurrentLinkedDeque<NodeRef> nodeRefs) {
    this.nodeRefs = nodeRefs;
    installGCMonitoring();
  }

  /** monitor GC, and should the heap grow above 80% usage, clear some strong references */
   protected void installGCMonitoring() {
    System.out.println("ReferenceManager.installGCMonitoring");
    Set<String> ignoredMemoryAreas = new HashSet<>(Arrays.asList("Code Cache", "Compressed Class Space", "Metaspace"));

    List<GarbageCollectorMXBean> gcbeans = java.lang.management.ManagementFactory.getGarbageCollectorMXBeans();
    for (GarbageCollectorMXBean gcbean : gcbeans) {
      NotificationListener listener = (notification, handback) -> {
        if (notification.getType().equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION)) {
          GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo.from((CompositeData) notification.getUserData());

          //sum up used and max memory across relevant memory areas
          long totalMemUsed = 0;
          long totalMemMax = 0;
          for (Map.Entry<String, MemoryUsage> entry : info.getGcInfo().getMemoryUsageAfterGc().entrySet()) {
            String name = entry.getKey();
            if (!ignoredMemoryAreas.contains(name)) {
              MemoryUsage detail = entry.getValue();
              totalMemUsed += detail.getUsed();
              totalMemMax += detail.getMax();
            }
          }
          float heapUsageBefore = (float) totalMemUsed / (float) totalMemMax;
          if (heapUsageBefore > 0.8) { //TODO make configurable
            System.out.println("heap usage (after GC) is " + heapUsageBefore + ", clearing some references");
            clearReferences();
          }
        }
      };
      NotificationEmitter emitter = (NotificationEmitter) gcbean;
      emitter.addNotificationListener(listener, null, null);
    }
  }

  protected void clearReferences() {
    int releaseCount = 20000; //TODO make configurable
    System.out.println("ReferenceManager: clearing " + releaseCount + " references");
    Iterator<NodeRef> iterator = nodeRefs.iterator();
    // TODO iterate differently every time to ensure it's not getting slower every time?
    //  e.g. random iteration would be ok?
    //  BETTER: remember last index, but ensure to start from bottom again
    //  alternative: keep separate list of to-be-cleared elements - would need to be kept in sync with graph, though (e.g. when elements are removed)
    while (iterator.hasNext() && releaseCount > 0) {
      NodeRef ref = iterator.next();
      if (ref.isSet()) {
        ref.clear();
        releaseCount--;
      }
    }
  }

}
