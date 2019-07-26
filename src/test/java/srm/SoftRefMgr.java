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
//package srm;
//
//import com.sun.management.GarbageCollectionNotificationInfo;
//
//import javax.management.ListenerNotFoundException;
//import javax.management.Notification;
//import javax.management.NotificationEmitter;
//import javax.management.NotificationListener;
//import javax.management.openmbean.CompositeData;
//import java.lang.management.GarbageCollectorMXBean;
//import java.lang.management.MemoryUsage;
//import java.lang.ref.SoftReference;
//import java.util.*;
//import java.util.concurrent.ConcurrentLinkedDeque;
//
//public class SoftRefMgr {
//
//  /** will run forever thanks to soft refs
//   * run with -XX:+UseG1GC -Xms256m -Xmx256m -XX:SoftRefLRUPolicyMSPerMB=0 */
//  public static void main(String[] args) throws Exception {
////    installGCMonitoring();
//    int numberOfNodes = 200000;
//    SoftReferenceManager softReferenceManager = new SoftReferenceManager();
//    ArrayList<NodeRef> nodeRefs = new ArrayList<>(numberOfNodes);
////    ArrayList<Node> nodes = new ArrayList<>(numberOfNodes);
//    for (long id = 0; id < numberOfNodes; id++) {
//      byte[] data = new byte[1024 * 2];
//      Node node = new Node(id, data);
//      nodeRefs.add(new NodeRef(node));
//      //      nodes.add(node);
//      softReferenceManager.register(node);
//
//      long count = id + 1;
//      if (count % 5000 == 0) {
//        long clearedSoftRefs = 0;
//        for (NodeRef ref : nodeRefs) {
//          if (ref.softReference.get() == null) {
//            clearedSoftRefs++;
//          }
//        }
//        System.out.println("created " + count + " nodes; cleared softrefs=" + clearedSoftRefs);
//        Thread.sleep(100);
//      }
//    }
//  }
//
//  public static void installGCMonitoring(){
//    Set<String> ignoredMemoryAreas = new HashSet<>(Arrays.asList("Code Cache", "Compressed Class Space", "Metaspace"));
//
//    List<GarbageCollectorMXBean> gcbeans = java.lang.management.ManagementFactory.getGarbageCollectorMXBeans();
//    for (GarbageCollectorMXBean gcbean : gcbeans) {
//      NotificationEmitter emitter = (NotificationEmitter) gcbean;
//      NotificationListener listener = new NotificationListener() {
//        @Override
//        public void handleNotification(Notification notification, Object handback) {
//          if (notification.getType().equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION)) {
//            GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo.from((CompositeData) notification.getUserData());
//
//            //sum up used and max memory across relevant memory areas
//            Map<String, MemoryUsage> memBeforeGc = info.getGcInfo().getMemoryUsageBeforeGc();
//            long totalMemUsed = 0;
//            long totalMemMax = 0;
//            for (Map.Entry<String, MemoryUsage> entry : memBeforeGc.entrySet()) {
//              String name = entry.getKey();
//              if (!ignoredMemoryAreas.contains(name)) {
//                MemoryUsage detail = entry.getValue();
//                totalMemUsed += detail.getUsed();
//                totalMemMax += detail.getMax();
//              }
//            }
//            float heapUsageBefore = (float) totalMemUsed / (float) totalMemMax;
//            // if heap usage before GC is >80%, let's clear some soft references, just to be on the safe side
//            System.out.println("heap usage (before GC): " + heapUsageBefore);
//          }
//        }
//      };
//
//      emitter.addNotificationListener(listener, null, null);
//    }
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
//class NodeRef {
//  public final long id;
//  public SoftReference<Node> softReference;
//
//  NodeRef(Node node) {
//    this.id = node.getId();
//    this.softReference = new SoftReference<>(node);
//  }
//}
//
//// optimisation: ensure not *all* soft references are released, by hanging onto all but a few of them
//class SoftReferenceManager {
//  public final int maxWildSoftReferences = 10000;
//  protected int observedElementCount = 0;
//  protected final ConcurrentLinkedDeque<Node> strongRefs = new ConcurrentLinkedDeque<>(); // `iterator.remove(Object)` is O(1)
//
//  public SoftReferenceManager() {
//    installGCMonitoring();
//  }
//
//  /** called from Element's constructor */
//  public void register(Node element) {
//    observedElementCount++;
//    if (observedElementCount > maxWildSoftReferences) {
//      // hold onto a strong reference to this element, so that it doesn't get cleared by the GC when we're low on memory
//      // element will be added at the *end* of the queue
//      strongRefs.add(element);
//    }
//  }
//
//  /** monitor GC, and should the heap grow above 80% usage, free some strong references */
//   protected void installGCMonitoring() {
//    System.out.println("SoftReferenceManager.installGCMonitoring");
//    Set<String> ignoredMemoryAreas = new HashSet<>(Arrays.asList("Code Cache", "Compressed Class Space", "Metaspace"));
//
//    List<GarbageCollectorMXBean> gcbeans = java.lang.management.ManagementFactory.getGarbageCollectorMXBeans();
//    for (GarbageCollectorMXBean gcbean : gcbeans) {
//      NotificationListener listener = (notification, handback) -> {
//        if (notification.getType().equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION)) {
//          GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo.from((CompositeData) notification.getUserData());
//
//          //sum up used and max memory across relevant memory areas
//          Map<String, MemoryUsage> memBeforeGc = info.getGcInfo().getMemoryUsageBeforeGc();
//          long totalMemUsed = 0;
//          long totalMemMax = 0;
//          for (Map.Entry<String, MemoryUsage> entry : memBeforeGc.entrySet()) {
//            String name = entry.getKey();
//            if (!ignoredMemoryAreas.contains(name)) {
//              MemoryUsage detail = entry.getValue();
//              totalMemUsed += detail.getUsed();
//              totalMemMax += detail.getMax();
//            }
//          }
//          float heapUsageBefore = (float) totalMemUsed / (float) totalMemMax;
//          if (heapUsageBefore > 0.8) { //TODO make configurable
//            System.out.println("heap usage (before GC) is " + heapUsageBefore + ", clearing some strong references");
//            releaseSomeStrongReferences();
//          }
//        }
//      };
//      NotificationEmitter emitter = (NotificationEmitter) gcbean;
//      emitter.addNotificationListener(listener, null, null);
//    }
//  }
//
//  // TODO release a little less?
//  protected void releaseSomeStrongReferences() {
//    System.out.println("SoftReferenceManager: releasing " + 10000 + " strong references");
//    Iterator<Node> iterator = strongRefs.iterator();
//    int releaseCount = 10000; //TODO make configurable
//    while (iterator.hasNext() && releaseCount > 0) {
//      observedElementCount--;
//      releaseCount--;
//      iterator.next();
//      iterator.remove(); // element is removed from the *beginning* of the queue
//    }
//  }
//
//}
