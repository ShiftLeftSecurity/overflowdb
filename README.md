[![Build Status](https://travis-ci.org/ShiftLeftSecurity/overflowdb.svg?branch=master)](https://travis-ci.org/ShiftLeftSecurity/overflowdb)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.shiftleft/overflowdb-tinkerpop3/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.shiftleft/overflowdb-tinkerpop3)

## ShiftLeft OverflowDB
* in-memory graph database with low memory footprint
* overflows to disk when running out of heap space: use your entire heap and prevent `OutOfMemoryError`
* property graph model, i.e. there are **nodes** and **directed edges**, both of which can have properties
* work with simple classes, rather than abstracting over some generic model <!-- and using a query language a la sql/gremlin/cql/cypher/... -->
* enforces strict schema
* can save/load to/from disk

### Table of contents
<!-- generated with https://github.com/jonschlinkert/markdown-toc 
markdown-toc --maxdepth 2 --no-firsth1 README.md
-->
- [Core concepts](#core-concepts)
- [Usage](#usage)
- [Configuration](#configuration)
- [TinkerPop3 compatibility](#tinkerpop3-compatibility)
- [FAQ](#faq)

### Core concepts
**Memory layout**: edges only exist *virtually*, i.e. they *normally* don't exist as edge instances on your heap, 
and they do not have an ID. Instead, edges are helt in the `OdbNode.adjacentNodesWithProperties`, which is an `Object[]`, 
containing direct pointers to the adjacent nodes, as well as potential edge properties. Those edges are grouped by edge label, 
and there's a _helper_ array `OdbNode.edgeOffsets` to keep track of those group sizes.  
This model has been chosen in order to be memory efficient, and is based on the assumption that most graphs have orders of magnitude more edges than nodes.   

**Simple classes and schema**: all nodes/edges are *specific to your domain* rather than *generic with arbitrary properties*. 
This way we get a strict schema and don't waste memory on `Map` instances. 
As of today, TinkerPop3 is the only query language to interact with the graph. TinkerPop returns generic `Vertex|Edge` instances,
but if you want to access their properties in a type-safe way (`person.name` rather than `vertex.property("NAME")`, you can cast 
them to your specific node|edge based on their label. 

**Overflow mechanism**: for maximum throughput and simplicity, OverflowDB is designed to run on the same JVM as your 
main application. Since the memory requirements of your application will likely vary over time, OverflowDB dynamically adapts 
to the available memory. I.e. it will allocate instances on the heap while there's still space, but if the heap usage (after a full GC)
is above a configurable threashold (e.g. 80%), it will start serializing instances to disk, freeing up some space. 
This way we can always fully utilize the heap while preventing `OutOfMemoryError`. OverflowDB applies backpressure to creating 
new nodes in that case. These mechanisms have practically no overhead while there is enough heap available and the overflow is not required.  

**Persistence**
* save to disk, load from disk.
* graphLocation - auto save
* only persists to disk on proper close. there's no guarantees what happens on jvm crash.

### Usage
1) add a dependency to the latest published artifact on [maven central](https://maven-badges.herokuapp.com/maven-central/io.shiftleft/overflowdb)
TODO
<!-- 2) extend [SpecializedTinkerVertex](https://github.com/ShiftLeftSecurity/tinkergraph-gremlin/blob/master/src/main/java/org/apache/tinkerpop/gremlin/tinkergraph/structure/SpecializedTinkerVertex.java) for vertices and [SpecializedTinkerEdge](https://github.com/ShiftLeftSecurity/tinkergraph-gremlin/blob/master/src/main/java/org/apache/tinkerpop/gremlin/tinkergraph/structure/SpecializedTinkerEdge.java) for edges 
3) create instances of [`SpecializedElementFactory.ForVertex`](https://github.com/ShiftLeftSecurity/tinkergraph-gremlin/blob/master/src/main/java/org/apache/tinkerpop/gremlin/tinkergraph/structure/SpecializedElementFactory.java#L29) and [`SpecializedElementFactory.ForEdge`](https://github.com/ShiftLeftSecurity/tinkergraph-gremlin/blob/master/src/main/java/org/apache/tinkerpop/gremlin/tinkergraph/structure/SpecializedElementFactory.java#L34) and pass them to [`TinkerGraph.open`](https://github.com/ShiftLeftSecurity/tinkergraph-gremlin/blob/master/src/main/java/org/apache/tinkerpop/gremlin/tinkergraph/structure/TinkerGraph.java#L153-L156)
-->

### Configuration
* Overflow heap config 
* graphLocation - auto save
    * only persists to disk on proper close. there's no guarantees what happens on jvm crash
    
### Overflow implementation
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

### TinkerPop3 compatibility
While this project originally started as a [Fork of TinkerGraph](https://github.com/ShiftLeftSecurity/tinkergraph-gremlin/), 
it has diverged significantly. While most traversals *should* still work, there may be some that don't. The most obvious thing 
that doesn't work is starting a traversal with an edge, e.g. by `g.E(0).toList` - that's because edges only exist virtually, 
so they don't have IDs and can't be indexed. There's no inherent reason this can't be done, but the need didn't yet arise. 
Same goes for an OLAP (GraphComputer) implementation, which is not yet available.

### FAQ
1. **Why not just use a simple cache instead of the overflow mechanism?**  
Regular caches require you have to specify a fixed size. OverflowDB is designed to run in the same JVM as your main application, and since 
most applications have varying memory needs over time, it would be hard/impossible to achieve our goal *use your entire heap and prevent OutOfMemoryError* 
with a regular cache. Besides that, it's very compute-intensive to calculate the size of the cache in megabytes on the heap. 
1. **When is the next release coming out?**  
Releases happen automatically. Every PR merged to master is automatically released by travis.ci and tagged in git, using [sbt-ci-release-early](https://github.com/ShiftLeftSecurity/sbt-ci-release-early)
1. **What repositories are the artifacts deployed to?**   
https://oss.sonatype.org/content/repositories/public/io/shiftleft/overflowdb-tinkerpop3/
https://repo1.maven.org/maven2/io/shiftleft/overflowdb-tinkerpop3/
