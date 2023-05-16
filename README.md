[![Release](https://github.com/ShiftLeftSecurity/overflowdb/actions/workflows/release.yml/badge.svg)](https://github.com/ShiftLeftSecurity/overflowdb/actions/workflows/release.yml)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.shiftleft/overflowdb-traversal_2.13/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.shiftleft/overflowdb-traversal_2.13)

## ShiftLeft OverflowDB
* in-memory graph database with low memory footprint
* overflows to disk when running out of heap space: use your entire heap and prevent `OutOfMemoryError`
* property graph model, i.e. there are **nodes** and **directed edges**, both of which can have properties
* work with simple classes, rather than abstracting over some generic model <!-- and using a query language a la sql/gremlin/cql/cypher/... -->
* enforces strict schema
* can save/load to/from disk
* [built on JDK11](https://github.com/ShiftLeftSecurity/overflowdb/blob/master/.github/workflows/release.yml) but with release target=8, i.e. runs on JRE >= 1.8

### Table of contents
<!--  
markdown-toc --maxdepth 2 --no-firsth1 README.md
https://github.com/jonschlinkert/markdown-toc
-->
- [Core concepts](#core-concepts)
- [Usage](#usage)
- [Configuration](#configuration)
- [FAQ](#faq)

### Core concepts
**Memory layout**: edges only exist *virtually*, i.e. they *normally* don't exist as edge instances on your heap, 
and they do not have an ID. Instead, edges are helt in the `NodeDb.adjacentNodesWithEdgeProperties`, which is an `Object[]`, 
containing direct pointers to the adjacent nodes, as well as potential edge properties. Those edges are grouped by edge label, 
and there's a _helper_ array `NodeDb.edgeOffsets` to keep track of those group sizes.  
This model has been chosen in order to be memory efficient, and is based on the assumption that most graphs have orders of magnitude more edges than nodes.   

**Simple classes and schema**: all nodes/edges are *specific to your domain* rather than *generic with arbitrary properties*. 
This way we get a strict schema and don't waste memory on `Map` instances. On the flip side, you need to provide your domain-specific
`[Node|Edge]Factories` to instantiate them. These can be auto-generated though, and we may provide a codegen in future. 

**Overflow**: for maximum throughput and simplicity, OverflowDB is designed to run on the same JVM as your 
main application. Since the memory requirements of your application will likely vary over time, OverflowDB dynamically adapts 
to the available memory. I.e. it will allocate instances on the heap while there's still space, but if the heap usage (after a full GC)
is above a configurable threashold (e.g. 80%), it will start serializing instances to disk, freeing up some space. 
This way we can always fully utilize the heap while preventing `OutOfMemoryError`. OverflowDB applies backpressure to creating 
new nodes in that case. These mechanisms have practically no overhead while there is enough heap available and the overflow is not required.  

**Persistence**: if you provide a `graphLocation` when creating the graph, OverflowDB will a) use that file for the on-disk overflow,
and b) persist to that location on `graph.close()`. 'Persisting' is equivalent to simply serializing all nodes to disk, via the 
normal 'overflow' mechanism.  
If the `graphLocation` file already exists, OverflowDB will initialize all NodeRefs from it. I.e. starting up is fast, but the first
 queries will be slow, until all required nodes are deserialized from disk. 
Note that there's no guarantees what happens on jvm crash.

### Usage
**1)** add a dependency - depending on your build tool. Latest release: [![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.shiftleft/overflowdb-traversal_2.13/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.shiftleft/overflowdb-traversal_2.13)
```scala
libraryDependencies += "io.shiftleft" %% "overflowdb-traversal" % "x.y" // sbt
```
```xml
<dependency> <!-- maven -->
  <groupId>io.shiftleft</groupId>
  <artifactId>overflowdb-traversal_2.13</artifactId>
  <version>x.y</version>
</dependency>
```
```groovy
implementation 'io.shiftleft:overflowdb-traversal_2.13:x.y' // gradle
```
[Other build tools and versions](https://search.maven.org/search?q=g:io.shiftleft%20AND%20a:overflowdb-traversal_2.13&core=gav)

**2)** Implement your domain-specific nodes/edges and factories. It's probably best to follow the example implementations 
of [simple](https://github.com/ShiftLeftSecurity/overflowdb/tree/master/tinkerpop3/src/test/java/io/shiftleft/overflowdb/testdomains/simple) 
and [grateful dead](https://github.com/ShiftLeftSecurity/overflowdb/tree/master/tinkerpop3/src/test/java/io/shiftleft/overflowdb/testdomains/gratefuldead).

**3)** Create a graph
```java
OdbGraph graph = OdbGraph.open(
  OdbConfig.withoutOverflow(),
  Arrays.asList(Song.factory, Artist.factory),
  Arrays.asList(FollowedBy.factory, SungBy.factory, WrittenBy.factory)
);

// either create some nodes/edges manually
Song song1 = (Song) graph.addVertex(T.label, Song.label, Song.NAME, "Song 1");
Song song2 = (Song) graph.addVertex(T.label, Song.label, Song.NAME, "Song 2");
song1.addEdge(FollowedBy.LABEL, song2);

// or import e.g. a graphml
GraphML.insert("src/test/resources/grateful-dead.xml", graph);
```

**4)** Traverse for fun and profit
```java
assertEquals(Long.valueOf(808), graph.traversal().V().count().next());
assertEquals(Long.valueOf(8049), graph.traversal().V().outE().count().next());

Artist garcia = (Artist) graph.traversal().V().has("name", "Garcia").next();
assertEquals("Garcia", garcia.name);
assertEquals(4, __(garcia).in(WrittenBy.LABEL).toList().size());
```

**5)** `graph.close`

For more complete examples, please check out the [tests](https://github.com/ShiftLeftSecurity/overflowdb/tree/master/tinkerpop3/src/test/java/io/shiftleft/overflowdb).  
To learn more about traversals please refer to the [TinkerPop3 documentation](http://tinkerpop.apache.org/docs/current/reference/).

### Configuration: OdbConfig builder
```java
OdbConfig config = OdbConfig.withDefaults()   // overflow is enabled, threshold is 80% of heap (after full GC)
config.disableOverflow // or shorter: OdbConfig.withoutOverflow() 
config.withHeapPercentageThreshold(90)        // set threshold to 90% (after full GC)

// relative or absolute path to storage
// if specified, OverflowDB will persist to that location on `graph.close()`
// to restore from that location, simply instantiate a new graph instance with the same setting 
config.withStorageLocation("path/to/odb.bin") 
```
    
### Overflow mechanism
Here's a rough sketch of how the overflow mechanism works internally: <!-- http://asciiflow.com -->
```
+----------+        +--------------+         +-----------------------+
|          |        |   NodeRef    |  free!  |    Node               |
|OdbStorage+--------+              +-------->+                       |
|          |        |String name();|         |String name;           |
|          |        |              |         |Object[] adjacentNodes;|
+----------+        +------+-------+         +-----------------------+
                           ^
                           |
                           |free!
                           |
                   +-------+--------+
                   |ReferenceManager|
                   +-------+--------+
                           ^
                           |
                           |free!
                           |
                     +-----+-----+
                     |HeapMonitor|
                     +-----------+

```
`NodeRef` instances have a low memory footprint - they only contain the `id` and a reference to the graph - and can be freely passed 
around the application. `Node`s in contrast hold all properties, as well as the adjacent edges and their properties. When the available
heap is getting low, it is the `Node` instances that are serialized to disk and collected by the garbage collector. That's why you should 
never hold a (strong) reference onto them in your main application: it would inhibit the overflow mechanism.   

### DiffTool util
`overflowdb.util.DiffTool.compare(graph1, graph2)` allows you to do some very basic comparison of two graphs. It identifies nodes by their ids, and compares their existence, properties, adjacent edges and properties of those edges. 

### FAQ
1. **What JDK does OverflowDB support?**
The build targets JDK8, so that's the minimum version. However it is highly encouraged to use a modern JVM, such as JDK20. 
The build itself requires JDK11+ though. 

1. **Why not just use a simple cache instead of the overflow mechanism?**  
Regular caches require you have to specify a fixed size. OverflowDB is designed to run in the same JVM as your main application, and since 
most applications have varying memory needs over time, it would be hard/impossible to achieve our goal *use your entire heap and prevent OutOfMemoryError* 
with a regular cache. Besides that, it's very compute-intensive to calculate the size of the cache in megabytes on the heap. 
1. **When is the next release coming out?**  
Releases happen automatically. Every PR merged to master is automatically released by github actions and tagged in git, using [sbt-ci-release-early](https://github.com/ShiftLeftSecurity/sbt-ci-release-early)
