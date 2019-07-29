[![Build Status](https://travis-ci.org/ShiftLeftSecurity/overflowdb.svg?branch=master)](https://travis-ci.org/ShiftLeftSecurity/overflowdb)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.shiftleft/overflowdb/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.shiftleft/overflowdb)

# ShiftLeft OverflowDB
* jvm-embedded property graph database
* low memory footprint
* automatically overflows to disk when heap space is running low
* if no serialization is required, it is completely in memory and has zero overhead 
* work with simple classes, rather than abstracting over some model and using a query language a la sql/gremlin/cql/cypher/...
* strict schema
* can save/load to/from disk
* this originally started as a [Fork of TinkerGraph](https://github.com/ShiftLeftSecurity/tinkergraph-gremlin/)
* partly compatible with TinkerPop3 API
<!-- * every PR merged to master is automatically released by travis.ci and tagged in git -->

## Usage
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

## TinkerPop3 compatibility
* we (currently) do not run the entire tinkerpop standard testsuite
* edges only exist virtually and therefor don't have an id that they can be looked up or indexed by
* an OLAP (GraphComputer) implementation is not available

## Limitations
* indices aren't updated automatically when you mutate or add elements to the graph. This would be easy to do I guess, but we haven't had the need yet. Workaround: drop and recreate the index.

