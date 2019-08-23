[![Build Status](https://travis-ci.org/ShiftLeftSecurity/overflowdb.svg?branch=master)](https://travis-ci.org/ShiftLeftSecurity/overflowdb)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.shiftleft/overflowdb-tinkerpop3/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.shiftleft/overflowdb-tinkerpop3)

## ShiftLeft OverflowDB
* in-memory graph database with low memory footprint
* overflows to disk when running out of heap space (preventing `OutOfMemoryError`)
* property graph model, i.e. there are **nodes** and **directed edges**, both of which can have properties
* work with simple classes, rather than abstracting over some model and using a query language a la sql/gremlin/cql/cypher/...
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
In order to be memory efficient, edges only exist *virtually*, i.e. they *normally* don't exist as edge instances on your heap, 
and they do not have an ID. Instead, edges are represented in `adjacentNodesWithProperties` 

Therefor it's typically be to embed OverflowDB in your JVM as part of the 

TODO continue
* Overflow mechanism: swap
    * if overflow is not required, it is completely in memory and has zero overhead
    * make use of your entire heap - your application as well as the db
    * Refs
    
* save to disk, load from disk
    * graphLocation - auto save
    * only persists to disk on proper close. there's no guarantees what happens on jvm crash

### Usage
1) add a dependency to the latest published artifact on [maven central](https://maven-badges.herokuapp.com/maven-central/io.shiftleft/overflowdb)
TODO

<!-- 2) extend [SpecializedTinkerVertex](https://github.com/ShiftLeftSecurity/tinkergraph-gremlin/blob/master/src/main/java/org/apache/tinkerpop/gremlin/tinkergraph/structure/SpecializedTinkerVertex.java) for vertices and [SpecializedTinkerEdge](https://github.com/ShiftLeftSecurity/tinkergraph-gremlin/blob/master/src/main/java/org/apache/tinkerpop/gremlin/tinkergraph/structure/SpecializedTinkerEdge.java) for edges 
3) create instances of [`SpecializedElementFactory.ForVertex`](https://github.com/ShiftLeftSecurity/tinkergraph-gremlin/blob/master/src/main/java/org/apache/tinkerpop/gremlin/tinkergraph/structure/SpecializedElementFactory.java#L29) and [`SpecializedElementFactory.ForEdge`](https://github.com/ShiftLeftSecurity/tinkergraph-gremlin/blob/master/src/main/java/org/apache/tinkerpop/gremlin/tinkergraph/structure/SpecializedElementFactory.java#L34) and pass them to [`TinkerGraph.open`](https://github.com/ShiftLeftSecurity/tinkergraph-gremlin/blob/master/src/main/java/org/apache/tinkerpop/gremlin/tinkergraph/structure/TinkerGraph.java#L153-L156)
-->

The repository contains examples for the [grateful dead graph](https://github.com/ShiftLeftSecurity/overflowdb/tree/michael/master/src/test/java/io/shiftleft/overflowdb/structure/specialized/gratefuldead/GratefulGraphTest.java).
<!-- 2) and 3) are basically boilerplate and therefor good candidates for code generation.  -->

<!-- # Motivation and context -->
<!-- The main difference is that instead of generic HashMaps we use specific structures as per your domain. To make this more clear, let's look at the main use cases for HashMaps in TinkerGraph: -->

<!-- 1) allow any vertex and any edge to have any property (basically a key/value pair, e.g., `foo=42`). To achieve this, each element in the graph has a `Map<String, Property>`, and each property is wrapped inside a `HashMap$Node`, see [TinkerVertex](https://github.com/apache/tinkerpop/blob/3.3.0/tinkergraph-gremlin/src/main/java/org/apache/tinkerpop/gremlin/tinkergraph/structure/TinkerVertex.java#L45) and [TinkerEdge](https://github.com/apache/tinkerpop/blob/3.3.0/tinkergraph-gremlin/src/main/java/org/apache/tinkerpop/gremlin/tinkergraph/structure/TinkerEdge.java#L43).  -->
<!-- 2) TinkerGraph allows to connect any two vertices by any edge. Therefor each vertex holds two `Map<String, Set<Edge>>` instances (one for incoming and one for outgoing edges), where the String refers to the edge label. -->

<!-- Being generic and not enforcing a schema makes complete sense for the default TinkerGraph - it allows users to play without restrictions and build prototypes. Once a project is more mature though, chances are you have a good understanding of your domain and can define a schema, so that you don't need the generic structure any more and can save a lot of memory. -->

<!-- Using less memory is not the only benefit, though: knowing exactly which properties a given element can have, of which type they are and which edges are allowed on a specific vertex, helps catching errors very early in the development cycle. Your IDE can help you to build valid (i.e., schema conforming) graphs and traversals. If you use a statically-checked language, your compiler can find errors that would otherwise only occur at runtime. Even if you are using a dynamic language you are better off, because you'll get an error when you load the graph, e.g., by setting a property on the wrong vertex type. This is far better than getting invalid results at query time, when you need to debug all the way back to a potentially very simple mistake. Since we already had a loosely-defined schema for our code property graph, this exercise helped to complete and strengthen it. -->

<!-- ## What does this mean in practice? -->
<!-- 'Enforcing a strict schema' actually translates to something very simple: we just replaced the *generic* HashMaps with *specific* members: -->

<!-- 1) Element properties: vertices and edges contain *generic* `HashMap<String, Object>` that hold all the element's properties. We just replaced them with *specific* class members, e.g., `String name` and `String return_type` -->

<!-- 2) Edges on a vertex: the *generic* TinkerVertex contains two `HashMap<String, Set<Edge>> in|outEdges` which can reference any edge. We replaced these by *specific* `Set<SomeSpecificEdgeType>` for each edge type that is allowed to connect this vertex with another vertex. -->

<!-- This means that we can throw an error if the schema is violated, e.g., if a the user tries to set a property that is not defined for a specific vertex, or if the user tris to connect a vertex via an edge that's not supposed to be connected to this vertex.  -->
<!-- It is important to note though, that it's up to you if you want to make this a strict validation or not - you can choose to tolerate schema violations in your domain classes. -->

### Configuration
* Overflow heap config 
* graphLocation - auto save
    * only persists to disk on proper close. there's no guarantees what happens on jvm crash


### TinkerPop3 compatibility
While this project originally started as a [Fork of TinkerGraph](https://github.com/ShiftLeftSecurity/tinkergraph-gremlin/), 
it has diverged significantly. While most traversals *should* still work, there may be some that don't. The most obvious thing 
that doesn't work is starting a traversal with an edge, e.g. by `g.E(0).toList` - that's because edges only exist virtually, 
so they don't have IDs and can't be indexed. There's no inherent reason this can't be done, but the need didn't yet arise. 
Same goes for an OLAP (GraphComputer) implementation, which is not yet available.

### Memory layout
TODO 

### FAQ
1. **Can you please do a release?**  
Releases happen automatically. Every PR merged to master is automatically released by travis.ci and tagged in git, using [sbt-ci-release-early](https://github.com/ShiftLeftSecurity/sbt-ci-release-early)
1. **What repositories are the artifacts deployed to?**   
https://oss.sonatype.org/content/repositories/public/io/shiftleft/overflowdb-tinkerpop3/
https://repo1.maven.org/maven2/io/shiftleft/overflowdb-tinkerpop3/
